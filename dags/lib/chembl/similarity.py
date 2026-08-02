"""Calculate Tanimoto similarity scores and top-N molecule matches."""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from rdkit import DataStructs

from lib.chembl.constants import (
    AWS_CONN_ID,
    DEFAULT_SOURCE_MOLECULE_LIMIT,
    DEFAULT_TOP_N,
    FINGERPRINTS_S3_PREFIX,
    S3_BUCKET,
    SIMILARITY_S3_PREFIX,
    SOURCE_INPUT_PREFIX,
    SOURCE_ID_COLUMNS,
    TOP10_FILE_NAME,
    TOP10_S3_KEY,
    TOP10_S3_PREFIX,
)
from lib.utils.parsing import normalize_chembl_id, parse_positive_int
from lib.utils.s3 import (
    build_s3_folder_prefix,
    delete_s3_prefix,
    download_s3_file,
    get_s3_client,
    list_s3_keys,
    normalize_s3_prefix,
    upload_s3_file,
)

logger = logging.getLogger(__name__)

FINGERPRINT_COLUMNS = [
    'chembl_id',
    'canonical_smiles',
    'fingerprint_binary',
]

TOP_N_COLUMNS = [
    'source_chembl_id',
    'target_chembl_id',
    'similarity_score',
    'similarity_rank',
    'has_duplicates_of_last_largest_score',
]

SOURCE_CSV_ENCODINGS = [
    'utf-8-sig',
    'utf-8',
    'utf-16',
    'utf-16-le',
    'utf-16-be',
    'cp1252',
]

SIMILARITY_SCHEMA = pa.schema(
    [
        ('source_chembl_id', pa.string()),
        ('target_chembl_id', pa.string()),
        ('similarity_score', pa.float64()),
    ]
)


@dataclass(frozen=True)
class SimilarityRunConfig:
    """Runtime configuration for similarity calculation."""

    source_input_prefix: str
    source_molecule_limit: int
    top_n: int

    @classmethod
    def from_params(
        cls,
        source_input_prefix: str,
        source_molecule_limit: int,
        top_n: int,
    ) -> SimilarityRunConfig:
        """Build validated config from DAG parameters."""
        return cls(
            source_input_prefix=normalize_s3_prefix(source_input_prefix),
            source_molecule_limit=parse_positive_int(
                source_molecule_limit,
                'source_molecule_limit',
            ),
            top_n=parse_positive_int(top_n, 'top_n'),
        )


@dataclass(frozen=True)
class SourceSimilaritySummary:
    """Summary metadata for one processed source molecule."""

    source_chembl_id: str
    targets_compared: int
    top_rows: int
    has_duplicates_of_last_largest_score: bool

    def to_dict(self) -> dict[str, int | str]:
        """Convert summary to Airflow-friendly dictionary."""
        return {
            'source_chembl_id': self.source_chembl_id,
            'targets_compared': self.targets_compared,
            'top_rows': self.top_rows,
            'has_duplicates_of_last_largest_score': int(
                self.has_duplicates_of_last_largest_score
            ),
        }


def normalize_column_name(column_name: str) -> str:
    """Normalize input CSV column names."""
    return column_name.lower().strip().replace(' ', '_')


def read_source_csv(local_path: Path, s3_key: str) -> pd.DataFrame:
    """Read source molecule CSV using common text encodings."""
    last_error: Exception | None = None

    for encoding in SOURCE_CSV_ENCODINGS:
        try:
            dataframe = pd.read_csv(
                local_path,
                encoding=encoding,
                dtype=str,
            )

            logger.info(
                'Read source molecule file %s using encoding=%s',
                s3_key,
                encoding,
            )

            return dataframe

        except UnicodeDecodeError as exc:
            last_error = exc

            logger.warning(
                'Could not read %s with encoding=%s. Trying next encoding.',
                s3_key,
                encoding,
            )

    raise ValueError(
        f'Could not read source molecule CSV file {s3_key} with supported '
        f'encodings: {SOURCE_CSV_ENCODINGS}'
    ) from last_error


def fingerprint_from_binary(fingerprint_binary: bytes):
    """Convert stored binary fingerprint back to an RDKit fingerprint."""
    return DataStructs.CreateFromBinaryText(bytes(fingerprint_binary))


def read_fingerprint_file(local_path: Path) -> pd.DataFrame:
    """Read one fingerprint parquet file and keep required columns."""
    dataframe = pd.read_parquet(local_path)

    missing_columns = [
        column
        for column in FINGERPRINT_COLUMNS
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            f'Fingerprint file {local_path} is missing columns: '
            f'{missing_columns}'
        )

    dataframe = dataframe[FINGERPRINT_COLUMNS].copy()
    dataframe['chembl_id'] = dataframe['chembl_id'].map(normalize_chembl_id)

    dataframe = dataframe.dropna(
        subset=[
            'chembl_id',
            'fingerprint_binary',
        ]
    )

    return dataframe


def read_source_chembl_ids_from_s3(
    s3_client: Any,
    source_input_prefix: str,
    temp_dir: Path,
) -> list[str]:
    """Read unique source molecule ChEMBL IDs from CSV files in S3."""
    if not source_input_prefix:
        logger.warning(
            'Source input prefix is empty. Fallback source selection will be used.'
        )
        return []

    source_input_folder_prefix = build_s3_folder_prefix(source_input_prefix)

    input_keys = list_s3_keys(
        s3_client=s3_client,
        bucket_name=S3_BUCKET,
        prefix=source_input_folder_prefix,
        suffix='.csv',
    )

    if not input_keys:
        logger.warning(
            'No source input CSV files found under s3://%s/%s. '
            'Fallback source selection will be used.',
            S3_BUCKET,
            source_input_folder_prefix,
        )
        return []

    source_ids: list[str] = []
    seen_ids: set[str] = set()

    for index, key in enumerate(input_keys):
        local_path = temp_dir / f'{index:05d}_{Path(key).name}'

        download_s3_file(
            s3_client=s3_client,
            bucket_name=S3_BUCKET,
            key=key,
            local_path=local_path,
        )

        dataframe = read_source_csv(
            local_path=local_path,
            s3_key=key,
        )

        if dataframe.empty:
            logger.warning('Skipping %s because the file is empty.', key)
            continue

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
                'Skipping %s because no supported ChEMBL ID column was found. '
                'Supported columns: %s',
                key,
                SOURCE_ID_COLUMNS,
            )
            continue

        for value in dataframe[source_column]:
            chembl_id = normalize_chembl_id(value)

            if chembl_id and chembl_id not in seen_ids:
                seen_ids.add(chembl_id)
                source_ids.append(chembl_id)

    if not source_ids:
        raise ValueError(
            'Source input CSV files were found, but no valid ChEMBL IDs were '
            f'read from s3://{S3_BUCKET}/{source_input_folder_prefix}. '
            f'Supported columns: {SOURCE_ID_COLUMNS}'
        )

    logger.info(
        'Loaded %s unique source molecule ID(s) from s3://%s/%s',
        len(source_ids),
        S3_BUCKET,
        source_input_folder_prefix,
    )

    return source_ids


def download_s3_files(
    s3_client: Any,
    keys: list[str],
    output_dir: Path,
) -> list[Path]:
    """Download S3 files to a temporary directory."""
    local_paths: list[Path] = []

    for index, key in enumerate(keys):
        local_path = output_dir / f'{index:05d}_{Path(key).name}'

        download_s3_file(
            s3_client=s3_client,
            bucket_name=S3_BUCKET,
            key=key,
            local_path=local_path,
        )

        local_paths.append(local_path)

    return local_paths


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
            dataframe = read_fingerprint_file(local_path)
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

        raise ValueError(
            'None of the input source molecules were found in the ChEMBL '
            'fingerprint files. Check that the input CSV contains ChEMBL IDs '
            'available in the current silver/fingerprint dataset.'
        )

    fallback_frames: list[pd.DataFrame] = []
    current_count = 0

    for local_path in fingerprint_file_paths:
        dataframe = read_fingerprint_file(local_path)
        fallback_frames.append(dataframe)
        current_count += len(dataframe)

        if current_count >= source_molecule_limit:
            break

    if not fallback_frames:
        raise ValueError('No fingerprint files were available for source selection.')

    logger.warning(
        'Using fallback source selection from available ChEMBL fingerprints.'
    )

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
    if top_candidates.empty:
        raise ValueError('Cannot add top-N metadata to empty dataframe.')

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


def build_similarity_s3_key(source_chembl_id: str) -> str:
    """Build S3 key for one source molecule similarity parquet file."""
    return (
        f'{SIMILARITY_S3_PREFIX}/'
        f'source_chembl_id={source_chembl_id}/'
        f'{source_chembl_id}_similarity_scores.parquet'
    )


def upload_similarity_file(
    s3_client: Any,
    local_similarity_path: Path,
    source_chembl_id: str,
) -> str:
    """Upload full similarity parquet file for one source molecule."""
    similarity_s3_key = build_similarity_s3_key(source_chembl_id)

    upload_s3_file(
        s3_client=s3_client,
        bucket_name=S3_BUCKET,
        local_path=local_similarity_path,
        key=similarity_s3_key,
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
    s3_client: Any,
) -> tuple[dict[str, int | str], list[dict[str, Any]]]:
    """Calculate full similarity table and top-N rows for one source molecule."""
    source_chembl_id = str(source_row['chembl_id'])
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
            target_dataframe = read_fingerprint_file(fingerprint_file_path)

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

    summary = SourceSimilaritySummary(
        source_chembl_id=source_chembl_id,
        targets_compared=target_count,
        top_rows=len(top_candidates),
        has_duplicates_of_last_largest_score=has_duplicates,
    )

    return summary.to_dict(), top_candidates.to_dict(orient='records')


def write_top10_results(
    top10_rows: list[dict[str, Any]],
    output_dir: Path,
    s3_client: Any,
) -> str:
    """Write combined top-N results to S3."""
    if not top10_rows:
        raise ValueError('No top-N rows were generated.')

    local_path = output_dir / TOP10_FILE_NAME

    dataframe = pd.DataFrame(top10_rows)
    dataframe = dataframe[TOP_N_COLUMNS]
    dataframe = dataframe.sort_values(
        ['source_chembl_id', 'similarity_rank']
    ).reset_index(drop=True)

    dataframe.to_parquet(
        local_path,
        engine='pyarrow',
        index=False,
    )

    upload_s3_file(
        s3_client=s3_client,
        bucket_name=S3_BUCKET,
        local_path=local_path,
        key=TOP10_S3_KEY,
    )

    logger.info(
        'Uploaded combined top-N results to s3://%s/%s',
        S3_BUCKET,
        TOP10_S3_KEY,
    )

    return TOP10_S3_KEY


class SimilarityCalculationService:
    """Calculate source-to-all similarities and combined top-N results."""

    def __init__(self, config: SimilarityRunConfig) -> None:
        self.config = config
        self.s3_client = get_s3_client(aws_conn_id=AWS_CONN_ID)

    def get_fingerprint_keys(self) -> list[str]:
        """List fingerprint parquet files from S3."""
        fingerprint_folder_prefix = build_s3_folder_prefix(
            FINGERPRINTS_S3_PREFIX
        )

        fingerprint_keys = list_s3_keys(
            s3_client=self.s3_client,
            bucket_name=S3_BUCKET,
            prefix=fingerprint_folder_prefix,
            suffix='.parquet',
        )

        if not fingerprint_keys:
            raise ValueError(
                f'No fingerprint parquet files found under '
                f'{fingerprint_folder_prefix}.'
            )

        logger.info('Found %s fingerprint parquet file(s).', len(fingerprint_keys))

        return fingerprint_keys

    def clear_existing_outputs(self) -> None:
        """Delete old similarity and top-N output files from S3."""
        delete_s3_prefix(
            s3_client=self.s3_client,
            bucket_name=S3_BUCKET,
            prefix=build_s3_folder_prefix(SIMILARITY_S3_PREFIX),
        )

        delete_s3_prefix(
            s3_client=self.s3_client,
            bucket_name=S3_BUCKET,
            prefix=build_s3_folder_prefix(TOP10_S3_PREFIX),
        )

    def select_source_molecules(
        self,
        fingerprint_file_paths: list[Path],
        temp_dir: Path,
    ) -> pd.DataFrame:
        """Select source molecules from input files or fallback selection."""
        source_chembl_ids = read_source_chembl_ids_from_s3(
            s3_client=self.s3_client,
            source_input_prefix=self.config.source_input_prefix,
            temp_dir=temp_dir,
        )

        source_dataframe = get_source_fingerprints_from_files(
            fingerprint_file_paths=fingerprint_file_paths,
            source_chembl_ids=source_chembl_ids,
            source_molecule_limit=self.config.source_molecule_limit,
        )

        if source_dataframe.empty:
            raise ValueError('No source molecules selected for similarity search.')

        logger.info(
            'Selected %s source molecule(s) for similarity calculation.',
            len(source_dataframe),
        )

        return source_dataframe

    def process_sources(
        self,
        source_dataframe: pd.DataFrame,
        fingerprint_file_paths: list[Path],
        output_dir: Path,
    ) -> tuple[list[dict[str, int | str]], list[dict[str, Any]]]:
        """Process all selected source molecules."""
        summaries: list[dict[str, int | str]] = []
        top10_rows: list[dict[str, Any]] = []

        for _, source_row in source_dataframe.iterrows():
            source_summary, source_top10_rows = process_source_molecule(
                source_row=source_row,
                fingerprint_file_paths=fingerprint_file_paths,
                output_dir=output_dir,
                top_n=self.config.top_n,
                s3_client=self.s3_client,
            )

            summaries.append(source_summary)
            top10_rows.extend(source_top10_rows)

        return summaries, top10_rows

    def run(self) -> dict[str, Any]:
        """Run the similarity calculation process."""
        fingerprint_keys = self.get_fingerprint_keys()
        self.clear_existing_outputs()

        with TemporaryDirectory(prefix='chembl_similarity_') as temp_dir_name:
            temp_dir = Path(temp_dir_name)

            fingerprint_file_paths = download_s3_files(
                s3_client=self.s3_client,
                keys=fingerprint_keys,
                output_dir=temp_dir,
            )

            source_dataframe = self.select_source_molecules(
                fingerprint_file_paths=fingerprint_file_paths,
                temp_dir=temp_dir,
            )

            output_dir = temp_dir / 'similarity_output'
            output_dir.mkdir(parents=True, exist_ok=True)

            summaries, top10_rows = self.process_sources(
                source_dataframe=source_dataframe,
                fingerprint_file_paths=fingerprint_file_paths,
                output_dir=output_dir,
            )

            top10_s3_key = write_top10_results(
                top10_rows=top10_rows,
                output_dir=output_dir,
                s3_client=self.s3_client,
            )

        return {
            'source_molecules': len(summaries),
            'top10_rows': len(top10_rows),
            'top10_s3_key': top10_s3_key,
        }


def compute_similarity_scores_and_top10(
    source_input_prefix: str = SOURCE_INPUT_PREFIX,
    source_molecule_limit: int = DEFAULT_SOURCE_MOLECULE_LIMIT,
    top_n: int = DEFAULT_TOP_N,
) -> dict[str, Any]:
    """Compute full similarity parquet files and combined top-N results."""
    config = SimilarityRunConfig.from_params(
        source_input_prefix=source_input_prefix,
        source_molecule_limit=source_molecule_limit,
        top_n=top_n,
    )

    logger.info(
        'Starting similarity calculation. source_input_prefix=%s, '
        'source_molecule_limit=%s, top_n=%s',
        config.source_input_prefix,
        config.source_molecule_limit,
        config.top_n,
    )

    result = SimilarityCalculationService(config=config).run()

    logger.info('Finished similarity calculation: %s', result)

    return result
