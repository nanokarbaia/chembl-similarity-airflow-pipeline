"""Calculate Tanimoto similarity scores and top-N molecule matches."""

from __future__ import annotations

import logging
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from rdkit import DataStructs

from lib.chembl.constants import (
    AWS_CONN_ID,
    DEFAULT_SOURCE_MOLECULE_LIMIT,
    DEFAULT_TOP_N,
    FINGERPRINTS_S3_PREFIX,
    S3_BUCKET,
    SIMILARITY_S3_PREFIX,
    SOURCE_INPUT_PREFIX,
    TOP10_S3_PREFIX,
)

logger = logging.getLogger(__name__)

FINGERPRINT_COLUMNS = [
    'chembl_id',
    'canonical_smiles',
    'fingerprint_binary',
]

SOURCE_ID_COLUMNS = [
    'chembl_id',
    'molecule_chembl_id',
    'source_chembl_id',
    'compound_chembl_id',
]

SIMILARITY_SCHEMA = pa.schema(
    [
        ('source_chembl_id', pa.string()),
        ('target_chembl_id', pa.string()),
        ('similarity_score', pa.float64()),
    ]
)


def normalize_chembl_id(value: Any) -> str | None:
    """Normalize ChEMBL ID values."""
    if value is None:
        return None

    normalized_value = str(value).strip().upper()

    if not normalized_value or normalized_value == 'NAN':
        return None

    return normalized_value


def normalize_column_name(column_name: str) -> str:
    """Normalize input CSV column names."""
    return column_name.lower().strip().replace(' ', '_')


def get_s3_client():
    """Create S3 client from Airflow connection."""
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    return s3_hook.get_conn()


def list_s3_keys(
    s3_client,
    prefix: str,
    suffix: str | None = None,
) -> list[str]:
    """List S3 keys under a prefix."""
    paginator = s3_client.get_paginator('list_objects_v2')
    keys: list[str] = []

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']

            if suffix and not key.endswith(suffix):
                continue

            keys.append(key)

    return sorted(keys)


def delete_s3_prefix(s3_client, prefix: str) -> None:
    """Delete all S3 objects under a prefix."""
    keys = list_s3_keys(s3_client=s3_client, prefix=prefix)

    if not keys:
        logger.info('No existing files found under s3://%s/%s', S3_BUCKET, prefix)
        return

    logger.info(
        'Deleting %s existing file(s) from s3://%s/%s',
        len(keys),
        S3_BUCKET,
        prefix,
    )

    for start_index in range(0, len(keys), 1000):
        batch = keys[start_index:start_index + 1000]
        s3_client.delete_objects(
            Bucket=S3_BUCKET,
            Delete={
                'Objects': [{'Key': key} for key in batch],
                'Quiet': True,
            },
        )


def download_s3_files(
    s3_client,
    keys: list[str],
    output_dir: Path,
) -> list[Path]:
    """Download S3 files to a temporary directory."""
    local_paths: list[Path] = []

    for index, key in enumerate(keys):
        local_path = output_dir / f'{index:05d}_{Path(key).name}'
        s3_client.download_file(S3_BUCKET, key, str(local_path))
        local_paths.append(local_path)

    return local_paths


def read_source_chembl_ids_from_s3(
    s3_client,
    source_input_prefix: str,
    temp_dir: Path,
) -> list[str]:
    """Read unique source molecule ChEMBL IDs from CSV files in S3."""
    input_keys = list_s3_keys(
        s3_client=s3_client,
        prefix=source_input_prefix,
        suffix='.csv',
    )

    if not input_keys:
        logger.warning(
            'No source input CSV files found under s3://%s/%s. '
            'Development fallback will be used.',
            S3_BUCKET,
            source_input_prefix,
        )
        return []

    source_ids: list[str] = []
    seen_ids: set[str] = set()

    for key in input_keys:
        local_path = temp_dir / Path(key).name
        s3_client.download_file(S3_BUCKET, key, str(local_path))

        dataframe = pd.read_csv(local_path)
        normalized_columns = {
            normalize_column_name(column): column
            for column in dataframe.columns
        }

        source_column = next(
            (
                normalized_columns[column]
                for column in SOURCE_ID_COLUMNS
                if column in normalized_columns
            ),
            None,
        )

        if source_column is None:
            logger.warning(
                'Skipping %s because no supported ChEMBL ID column was found.',
                key,
            )
            continue

        for value in dataframe[source_column]:
            chembl_id = normalize_chembl_id(value)

            if chembl_id and chembl_id not in seen_ids:
                seen_ids.add(chembl_id)
                source_ids.append(chembl_id)

    logger.info(
        'Loaded %s unique source molecule ID(s) from s3://%s/%s',
        len(source_ids),
        S3_BUCKET,
        source_input_prefix,
    )

    return source_ids


def fingerprint_from_binary(fingerprint_binary: bytes):
    """Convert stored binary fingerprint back to an RDKit fingerprint."""
    return DataStructs.CreateFromBinaryText(bytes(fingerprint_binary))


def get_source_fingerprints_from_files(
    fingerprint_file_paths: list[Path],
    source_chembl_ids: list[str],
    source_molecule_limit: int,
) -> pd.DataFrame:
    """Find source molecule fingerprints from fingerprint parquet files."""
    if source_chembl_ids:
        wanted_ids = set(source_chembl_ids)
        source_frames: list[pd.DataFrame] = []

        for local_path in fingerprint_file_paths:
            dataframe = pd.read_parquet(local_path, columns=FINGERPRINT_COLUMNS)
            matched_rows = dataframe[dataframe['chembl_id'].isin(wanted_ids)]

            if not matched_rows.empty:
                source_frames.append(matched_rows)

        if source_frames:
            source_dataframe = (
                pd.concat(source_frames, ignore_index=True)
                .drop_duplicates(subset=['chembl_id'])
            )

            source_order = {
                chembl_id: index
                for index, chembl_id in enumerate(source_chembl_ids)
            }

            source_dataframe['source_order'] = (
                source_dataframe['chembl_id'].map(source_order)
            )

            source_dataframe = source_dataframe.sort_values('source_order')
            source_dataframe = source_dataframe.drop(columns=['source_order'])

            if len(source_dataframe) < len(source_chembl_ids):
                logger.warning(
                    'Only %s out of %s input source molecules were found '
                    'in fingerprint files.',
                    len(source_dataframe),
                    len(source_chembl_ids),
                )

            return source_dataframe.head(source_molecule_limit)

        logger.warning(
            'None of the input source molecules were found in fingerprint files. '
            'Development fallback will be used.'
        )

    fallback_frames: list[pd.DataFrame] = []
    current_count = 0

    for local_path in fingerprint_file_paths:
        dataframe = pd.read_parquet(local_path, columns=FINGERPRINT_COLUMNS)
        fallback_frames.append(dataframe)
        current_count += len(dataframe)

        if current_count >= source_molecule_limit:
            break

    if not fallback_frames:
        raise ValueError('No fingerprint files were available for source selection.')

    return (
        pd.concat(fallback_frames, ignore_index=True)
        .drop_duplicates(subset=['chembl_id'])
        .head(source_molecule_limit)
    )


def calculate_similarity_chunk(
    source_chembl_id: str,
    source_fingerprint,
    target_dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate similarities for one source molecule against one target batch."""
    target_dataframe = target_dataframe[
        target_dataframe['chembl_id'] != source_chembl_id
    ].copy()

    if target_dataframe.empty:
        return pd.DataFrame(
            columns=[
                'source_chembl_id',
                'target_chembl_id',
                'similarity_score',
            ]
        )

    target_fingerprints = [
        fingerprint_from_binary(value)
        for value in target_dataframe['fingerprint_binary']
    ]

    similarity_scores = DataStructs.BulkTanimotoSimilarity(
        source_fingerprint,
        target_fingerprints,
    )

    return pd.DataFrame(
        {
            'source_chembl_id': source_chembl_id,
            'target_chembl_id': target_dataframe['chembl_id'].to_numpy(),
            'similarity_score': [
                round(float(score), 10)
                for score in similarity_scores
            ],
        }
    )


def update_top_candidates(
    current_top: pd.DataFrame | None,
    similarity_dataframe: pd.DataFrame,
    top_n: int,
) -> pd.DataFrame:
    """Keep only the best top-N candidates seen so far."""
    if current_top is None:
        candidates = similarity_dataframe
    else:
        candidates = pd.concat(
            [current_top, similarity_dataframe],
            ignore_index=True,
        )

    return (
        candidates.sort_values(
            ['similarity_score', 'target_chembl_id'],
            ascending=[False, True],
        )
        .head(top_n)
        .reset_index(drop=True)
    )


def write_similarity_chunk(
    writer: pq.ParquetWriter,
    similarity_dataframe: pd.DataFrame,
) -> None:
    """Append one similarity chunk to a parquet writer."""
    table = pa.Table.from_pandas(
        similarity_dataframe,
        schema=SIMILARITY_SCHEMA,
        preserve_index=False,
    )
    writer.write_table(table)


def add_top10_metadata(
    top_candidates: pd.DataFrame,
    score_counts: Counter[float],
) -> pd.DataFrame:
    """Add rank and duplicate-last-score flag to top-N candidates."""
    last_score = float(top_candidates.iloc[-1]['similarity_score'])

    selected_last_score_count = int(
        (top_candidates['similarity_score'] == last_score).sum()
    )

    total_last_score_count = score_counts[last_score]
    has_duplicates = total_last_score_count > selected_last_score_count

    result = top_candidates.copy()
    result['similarity_rank'] = range(1, len(result) + 1)
    result['has_duplicates_of_last_largest_score'] = (
        (result['similarity_score'] == last_score) & has_duplicates
    )

    return result


def upload_similarity_file(
    s3_client,
    local_similarity_path: Path,
    source_chembl_id: str,
) -> str:
    """Upload full similarity parquet file for one source molecule."""
    similarity_s3_key = (
        f'{SIMILARITY_S3_PREFIX}/'
        f'source_chembl_id={source_chembl_id}/'
        f'{source_chembl_id}_similarity_scores.parquet'
    )

    s3_client.upload_file(
        str(local_similarity_path),
        S3_BUCKET,
        similarity_s3_key,
    )

    logger.info(
        'Uploaded full similarity table for %s to s3://%s/%s',
        source_chembl_id,
        S3_BUCKET,
        similarity_s3_key,
    )

    return similarity_s3_key


def process_source_molecule(
    source_row: pd.Series,
    fingerprint_file_paths: list[Path],
    output_dir: Path,
    top_n: int,
    s3_client,
) -> tuple[dict[str, int | str], list[dict[str, Any]]]:
    """Calculate full similarity table and top-N rows for one source molecule."""
    source_chembl_id = source_row['chembl_id']
    source_fingerprint = fingerprint_from_binary(source_row['fingerprint_binary'])

    logger.info('Calculating similarities for source molecule: %s', source_chembl_id)

    local_similarity_path = output_dir / (
        f'{source_chembl_id}_similarity_scores.parquet'
    )

    parquet_writer = pq.ParquetWriter(
        local_similarity_path,
        SIMILARITY_SCHEMA,
    )

    top_candidates: pd.DataFrame | None = None
    score_counts: Counter[float] = Counter()
    target_count = 0

    try:
        for fingerprint_file_path in fingerprint_file_paths:
            target_dataframe = pd.read_parquet(
                fingerprint_file_path,
                columns=FINGERPRINT_COLUMNS,
            )

            similarity_dataframe = calculate_similarity_chunk(
                source_chembl_id=source_chembl_id,
                source_fingerprint=source_fingerprint,
                target_dataframe=target_dataframe,
            )

            if similarity_dataframe.empty:
                continue

            write_similarity_chunk(
                writer=parquet_writer,
                similarity_dataframe=similarity_dataframe,
            )

            target_count += len(similarity_dataframe)
            score_counts.update(similarity_dataframe['similarity_score'].tolist())

            top_candidates = update_top_candidates(
                current_top=top_candidates,
                similarity_dataframe=similarity_dataframe,
                top_n=top_n,
            )
    finally:
        parquet_writer.close()

    if top_candidates is None or top_candidates.empty:
        raise ValueError(f'No similarity scores calculated for {source_chembl_id}.')

    top_candidates = add_top10_metadata(
        top_candidates=top_candidates,
        score_counts=score_counts,
    )

    upload_similarity_file(
        s3_client=s3_client,
        local_similarity_path=local_similarity_path,
        source_chembl_id=source_chembl_id,
    )

    has_duplicates = bool(
        top_candidates['has_duplicates_of_last_largest_score'].any()
    )

    summary = {
        'source_chembl_id': source_chembl_id,
        'targets_compared': target_count,
        'top_rows': len(top_candidates),
        'has_duplicates_of_last_largest_score': int(has_duplicates),
    }

    return summary, top_candidates.to_dict(orient='records')


def write_top10_results(
    top10_rows: list[dict[str, Any]],
    output_dir: Path,
    s3_client,
) -> str:
    """Write combined top-10 results to S3."""
    if not top10_rows:
        raise ValueError('No top-10 rows were generated.')

    local_path = output_dir / 'top10_similar_molecules.parquet'

    dataframe = pd.DataFrame(top10_rows)
    dataframe = dataframe[
        [
            'source_chembl_id',
            'target_chembl_id',
            'similarity_score',
            'similarity_rank',
            'has_duplicates_of_last_largest_score',
        ]
    ]

    dataframe.to_parquet(
        local_path,
        engine='pyarrow',
        index=False,
    )

    s3_key = f'{TOP10_S3_PREFIX}/top10_similar_molecules.parquet'

    s3_client.upload_file(str(local_path), S3_BUCKET, s3_key)

    logger.info(
        'Uploaded combined top-10 results to s3://%s/%s',
        S3_BUCKET,
        s3_key,
    )

    return s3_key


def compute_similarity_scores_and_top10(
    source_input_prefix: str = SOURCE_INPUT_PREFIX,
    source_molecule_limit: int = DEFAULT_SOURCE_MOLECULE_LIMIT,
    top_n: int = DEFAULT_TOP_N,
) -> dict[str, Any]:
    """Compute full similarity parquet files and combined top-N results."""
    source_molecule_limit = int(source_molecule_limit)
    top_n = int(top_n)

    if source_molecule_limit < 1:
        raise ValueError('source_molecule_limit must be at least 1.')

    if top_n < 1:
        raise ValueError('top_n must be at least 1.')

    s3_client = get_s3_client()

    fingerprint_keys = list_s3_keys(
        s3_client=s3_client,
        prefix=FINGERPRINTS_S3_PREFIX,
        suffix='.parquet',
    )

    if not fingerprint_keys:
        raise ValueError(
            f'No fingerprint parquet files found under {FINGERPRINTS_S3_PREFIX}.'
        )

    logger.info('Found %s fingerprint parquet file(s).', len(fingerprint_keys))

    delete_s3_prefix(s3_client=s3_client, prefix=f'{SIMILARITY_S3_PREFIX}/')
    delete_s3_prefix(s3_client=s3_client, prefix=f'{TOP10_S3_PREFIX}/')

    with TemporaryDirectory(prefix='chembl_similarity_') as temp_dir_name:
        temp_dir = Path(temp_dir_name)

        source_chembl_ids = read_source_chembl_ids_from_s3(
            s3_client=s3_client,
            source_input_prefix=source_input_prefix,
            temp_dir=temp_dir,
        )

        fingerprint_file_paths = download_s3_files(
            s3_client=s3_client,
            keys=fingerprint_keys,
            output_dir=temp_dir,
        )

        source_dataframe = get_source_fingerprints_from_files(
            fingerprint_file_paths=fingerprint_file_paths,
            source_chembl_ids=source_chembl_ids,
            source_molecule_limit=source_molecule_limit,
        )

        logger.info(
            'Selected %s source molecule(s) for similarity calculation.',
            len(source_dataframe),
        )

        summaries = []
        top10_rows = []
        output_dir = temp_dir / 'similarity_output'
        output_dir.mkdir(parents=True, exist_ok=True)

        for _, source_row in source_dataframe.iterrows():
            source_summary, source_top10_rows = process_source_molecule(
                source_row=source_row,
                fingerprint_file_paths=fingerprint_file_paths,
                output_dir=output_dir,
                top_n=top_n,
                s3_client=s3_client,
            )

            summaries.append(source_summary)
            top10_rows.extend(source_top10_rows)

        top10_s3_key = write_top10_results(
            top10_rows=top10_rows,
            output_dir=output_dir,
            s3_client=s3_client,
        )

    result = {
        'source_molecules': len(summaries),
        'top10_rows': len(top10_rows),
        'top10_s3_key': top10_s3_key,
        'sources': summaries,
    }

    logger.info('Finished similarity calculation: %s', result)

    return result
