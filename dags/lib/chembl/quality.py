"""Data quality checks for the ChEMBL similarity pipeline."""

from __future__ import annotations

import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pandera.pandas as pa
from airflow.providers.postgres.hooks.postgres import PostgresHook

from lib.chembl.constants import (
    AWS_CONN_ID,
    DWH_CONN_ID,
    FINGERPRINT_N_BITS,
    FINGERPRINT_RADIUS,
    FINGERPRINTS_S3_PREFIX,
    S3_BUCKET,
    TOP10_S3_PREFIX,
)
from lib.utils.data_quality import (
    assert_equal,
    assert_positive,
    assert_zero,
    check_required_columns,
    fetch_single_value,
    get_relation_row_count,
)
from lib.utils.s3 import (
    download_s3_file,
    get_s3_client,
    list_s3_keys,
)

logger = logging.getLogger(__name__)

CHEMBL_ID_PATTERN = r'^CHEMBL[0-9]+$'

BRONZE_TABLES = [
    'chembl_id_lookup',
    'molecule_dictionary',
    'compound_properties',
    'compound_structures',
]

REQUIRED_DIM_COLUMNS = [
    'chembl_id',
    'molecule_type',
    'mw_freebase',
    'alogp',
    'psa',
    'cx_logp',
    'molecular_species',
    'full_mwt',
    'aromatic_rings',
    'heavy_atoms',
]

REQUIRED_FACT_COLUMNS = [
    'source_chembl_id',
    'target_chembl_id',
    'similarity_score',
    'similarity_rank',
    'has_duplicates_of_last_largest_score',
]

REQUIRED_GOLD_VIEWS = [
    'vw_avg_similarity_per_source',
    'vw_avg_alogp_deviation_per_source',
    'vw_similarity_pivot_10_sources',
    'vw_similarity_with_neighbors',
    'vw_avg_similarity_grouping_sets',
]

TOP10_S3_KEY = f'{TOP10_S3_PREFIX}/top10_similar_molecules.parquet'


CHEMBL_ID_CHECK = pa.Check(
    lambda series: series.astype(str).str.match(CHEMBL_ID_PATTERN),
    error='Column must contain valid ChEMBL IDs.',
)

FINGERPRINT_SCHEMA = pa.DataFrameSchema(
    {
        'chembl_id': pa.Column(str, nullable=False, checks=CHEMBL_ID_CHECK),
        'canonical_smiles': pa.Column(str, nullable=False),
        'fingerprint_binary': pa.Column(object, nullable=False),
        'fingerprint_on_bits': pa.Column(
            int,
            nullable=False,
            checks=[
                pa.Check.ge(0),
                pa.Check.le(FINGERPRINT_N_BITS),
            ],
        ),
        'fingerprint_radius': pa.Column(
            int,
            nullable=False,
            checks=pa.Check.equal_to(FINGERPRINT_RADIUS),
        ),
        'fingerprint_n_bits': pa.Column(
            int,
            nullable=False,
            checks=pa.Check.equal_to(FINGERPRINT_N_BITS),
        ),
    },
    strict=False,
    coerce=True,
)

TOP10_SCHEMA = pa.DataFrameSchema(
    {
        'source_chembl_id': pa.Column(
            str,
            nullable=False,
            checks=CHEMBL_ID_CHECK,
        ),
        'target_chembl_id': pa.Column(
            str,
            nullable=False,
            checks=CHEMBL_ID_CHECK,
        ),
        'similarity_score': pa.Column(
            float,
            nullable=False,
            checks=[
                pa.Check.ge(0),
                pa.Check.le(1),
            ],
        ),
        'similarity_rank': pa.Column(
            int,
            nullable=False,
            checks=pa.Check.ge(1),
        ),
        'has_duplicates_of_last_largest_score': pa.Column(
            bool,
            nullable=False,
            coerce=True,
        ),
    },
    strict=False,
    coerce=True,
)


def validate_pandera_schema(
    dataframe: pd.DataFrame,
    schema: pa.DataFrameSchema,
    check_name: str,
) -> pd.DataFrame:
    """Validate dataframe with Pandera and log useful failure details."""
    if dataframe.empty:
        raise ValueError(f'{check_name} failed. Dataframe is empty.')

    try:
        return schema.validate(dataframe, lazy=True)
    except pa.errors.SchemaErrors as exc:
        logger.error(
            '%s failed. Failure cases: %s',
            check_name,
            exc.failure_cases,
        )
        raise


def bronze_quality_checks() -> dict[str, int]:
    """Run quality checks for the bronze layer."""
    postgres_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    connection = postgres_hook.get_conn()
    results = {}

    try:
        with connection.cursor() as cursor:
            for table_name in BRONZE_TABLES:
                row_count = get_relation_row_count(
                    cursor=cursor,
                    schema_name='bronze',
                    relation_name=table_name,
                )

                assert_positive(
                    value=row_count,
                    check_name=f'bronze.{table_name} row count',
                )

                results[f'bronze_{table_name}_rows'] = row_count

    finally:
        connection.close()

    logger.info('Bronze quality checks passed: %s', results)

    return results


def silver_quality_checks() -> dict[str, int]:
    """Run quality checks for the silver molecule table."""
    postgres_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    connection = postgres_hook.get_conn()

    try:
        with connection.cursor() as cursor:
            silver_rows = get_relation_row_count(
                cursor=cursor,
                schema_name='silver',
                relation_name='molecules',
            )

            assert_positive(
                value=silver_rows,
                check_name='silver.molecules row count',
            )

            missing_required_values = fetch_single_value(
                cursor=cursor,
                query="""
                    SELECT COUNT(*)
                    FROM silver.molecules
                    WHERE chembl_id IS NULL
                       OR TRIM(chembl_id) = ''
                       OR canonical_smiles IS NULL
                       OR TRIM(canonical_smiles) = ''
                """,
            )

            assert_zero(
                value=missing_required_values,
                check_name='silver.molecules required values',
            )

            duplicate_chembl_ids = fetch_single_value(
                cursor=cursor,
                query="""
                    SELECT COUNT(*)
                    FROM (
                        SELECT chembl_id
                        FROM silver.molecules
                        GROUP BY chembl_id
                        HAVING COUNT(*) > 1
                    ) duplicates
                """,
            )

            assert_zero(
                value=duplicate_chembl_ids,
                check_name='silver.molecules duplicate chembl_id',
            )

            invalid_numeric_values = fetch_single_value(
                cursor=cursor,
                query="""
                    SELECT COUNT(*)
                    FROM silver.molecules
                    WHERE mw_freebase < 0
                       OR psa < 0
                       OR full_mwt < 0
                       OR aromatic_rings < 0
                       OR heavy_atoms < 0
                """,
            )

            assert_zero(
                value=invalid_numeric_values,
                check_name='silver.molecules invalid numeric values',
            )

    finally:
        connection.close()

    results = {
        'silver_molecules_rows': silver_rows,
    }

    logger.info('Silver quality checks passed: %s', results)

    return results


def fingerprint_quality_checks() -> dict[str, int]:
    """Run Pandera checks for fingerprint parquet files in S3."""
    s3_client = get_s3_client(aws_conn_id=AWS_CONN_ID)

    fingerprint_keys = list_s3_keys(
        s3_client=s3_client,
        bucket_name=S3_BUCKET,
        prefix=FINGERPRINTS_S3_PREFIX,
        suffix='.parquet',
    )

    if not fingerprint_keys:
        raise ValueError(
            f'No fingerprint parquet files found under {FINGERPRINTS_S3_PREFIX}.'
        )

    total_rows = 0

    with TemporaryDirectory(prefix='chembl_fingerprint_quality_') as temp_dir_name:
        temp_dir = Path(temp_dir_name)

        for index, key in enumerate(fingerprint_keys):
            local_path = temp_dir / f'fingerprints_{index:05d}.parquet'

            download_s3_file(
                s3_client=s3_client,
                bucket_name=S3_BUCKET,
                key=key,
                local_path=local_path,
            )

            dataframe = pd.read_parquet(local_path)
            validated_dataframe = validate_pandera_schema(
                dataframe=dataframe,
                schema=FINGERPRINT_SCHEMA,
                check_name=f'Fingerprint quality check for {key}',
            )

            total_rows += len(validated_dataframe)

    assert_positive(
        value=total_rows,
        check_name='fingerprint parquet total rows',
    )

    results = {
        'fingerprint_files': len(fingerprint_keys),
        'fingerprint_rows': total_rows,
    }

    logger.info('Fingerprint quality checks passed: %s', results)

    return results


def top10_quality_checks(top_n: int = 10) -> dict[str, int]:
    """Run Pandera checks for the combined top-N parquet file."""
    top_n = int(top_n)

    if top_n < 1:
        raise ValueError('top_n must be at least 1.')

    s3_client = get_s3_client(aws_conn_id=AWS_CONN_ID)

    with TemporaryDirectory(prefix='chembl_top10_quality_') as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        local_path = temp_dir / 'top10_similar_molecules.parquet'

        download_s3_file(
            s3_client=s3_client,
            bucket_name=S3_BUCKET,
            key=TOP10_S3_KEY,
            local_path=local_path,
        )

        dataframe = pd.read_parquet(local_path)

    validated_dataframe = validate_pandera_schema(
        dataframe=dataframe,
        schema=TOP10_SCHEMA,
        check_name='Top-N similarity quality check',
    )

    duplicate_pairs = validated_dataframe.duplicated(
        subset=['source_chembl_id', 'target_chembl_id'],
    )

    if duplicate_pairs.any():
        raise ValueError('Top-N file contains duplicate source-target pairs.')

    self_matches = validated_dataframe[
        validated_dataframe['source_chembl_id']
        == validated_dataframe['target_chembl_id']
    ]

    if not self_matches.empty:
        raise ValueError('Top-N file contains source-target self matches.')

    invalid_rank_groups = (
        validated_dataframe
        .groupby('source_chembl_id')
        .agg(
            row_count=('target_chembl_id', 'count'),
            min_rank=('similarity_rank', 'min'),
            max_rank=('similarity_rank', 'max'),
            distinct_ranks=('similarity_rank', 'nunique'),
        )
        .query(
            'row_count != @top_n '
            'or min_rank != 1 '
            'or max_rank != @top_n '
            'or distinct_ranks != @top_n'
        )
    )

    if not invalid_rank_groups.empty:
        raise ValueError(
            'Top-N file has invalid rank groups: '
            f'{invalid_rank_groups.to_dict(orient="index")}'
        )

    invalid_score_order_sources = []

    for source_chembl_id, source_rows in validated_dataframe.groupby(
        'source_chembl_id'
    ):
        ordered_rows = source_rows.sort_values('similarity_rank')

        if not ordered_rows['similarity_score'].is_monotonic_decreasing:
            invalid_score_order_sources.append(source_chembl_id)

    if invalid_score_order_sources:
        raise ValueError(
            'Top-N file has invalid similarity score order for sources: '
            f'{invalid_score_order_sources}'
        )

    results = {
        'top10_rows': len(validated_dataframe),
        'top10_source_molecules': (
            validated_dataframe['source_chembl_id'].nunique()
        ),
    }

    logger.info('Top-N quality checks passed: %s', results)

    return results


def gold_quality_checks(top_n: int = 10) -> dict[str, int]:
    """Run quality checks for gold tables and views."""
    top_n = int(top_n)

    if top_n < 1:
        raise ValueError('top_n must be at least 1.')

    postgres_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    connection = postgres_hook.get_conn()

    try:
        with connection.cursor() as cursor:
            check_required_columns(
                cursor=cursor,
                schema_name='gold',
                table_name='dim_molecule',
                required_columns=REQUIRED_DIM_COLUMNS,
            )

            check_required_columns(
                cursor=cursor,
                schema_name='gold',
                table_name='fact_molecule_similarity',
                required_columns=REQUIRED_FACT_COLUMNS,
            )

            fact_rows = get_relation_row_count(
                cursor=cursor,
                schema_name='gold',
                relation_name='fact_molecule_similarity',
            )

            dim_rows = get_relation_row_count(
                cursor=cursor,
                schema_name='gold',
                relation_name='dim_molecule',
            )

            source_count = fetch_single_value(
                cursor=cursor,
                query="""
                    SELECT COUNT(DISTINCT source_chembl_id)
                    FROM gold.fact_molecule_similarity
                """,
            )

            assert_positive(
                value=fact_rows,
                check_name='gold.fact_molecule_similarity row count',
            )

            assert_positive(
                value=dim_rows,
                check_name='gold.dim_molecule row count',
            )

            assert_positive(
                value=source_count,
                check_name='gold source molecule count',
            )

            assert_equal(
                actual_value=fact_rows,
                expected_value=source_count * top_n,
                check_name='gold fact rows = source_count * top_n',
            )

            invalid_rank_groups = fetch_single_value(
                cursor=cursor,
                query="""
                    SELECT COUNT(*)
                    FROM (
                        SELECT source_chembl_id
                        FROM gold.fact_molecule_similarity
                        GROUP BY source_chembl_id
                        HAVING COUNT(*) <> %s
                            OR MIN(similarity_rank) <> 1
                            OR MAX(similarity_rank) <> %s
                            OR COUNT(DISTINCT similarity_rank) <> %s
                    ) invalid_sources
                """,
                parameters=(top_n, top_n, top_n),
            )

            assert_zero(
                value=invalid_rank_groups,
                check_name='gold top_n rank groups',
            )

            invalid_scores = fetch_single_value(
                cursor=cursor,
                query="""
                    SELECT COUNT(*)
                    FROM gold.fact_molecule_similarity
                    WHERE similarity_score < 0
                       OR similarity_score > 1
                """,
            )

            assert_zero(
                value=invalid_scores,
                check_name='gold similarity score range',
            )

            self_matches = fetch_single_value(
                cursor=cursor,
                query="""
                    SELECT COUNT(*)
                    FROM gold.fact_molecule_similarity
                    WHERE source_chembl_id = target_chembl_id
                """,
            )

            assert_zero(
                value=self_matches,
                check_name='gold source-target self matches',
            )

            duplicate_fact_pairs = fetch_single_value(
                cursor=cursor,
                query="""
                    SELECT COUNT(*)
                    FROM (
                        SELECT source_chembl_id, target_chembl_id
                        FROM gold.fact_molecule_similarity
                        GROUP BY source_chembl_id, target_chembl_id
                        HAVING COUNT(*) > 1
                    ) duplicates
                """,
            )

            assert_zero(
                value=duplicate_fact_pairs,
                check_name='gold duplicate source-target pairs',
            )

            dim_molecules_not_used = fetch_single_value(
                cursor=cursor,
                query="""
                    SELECT COUNT(*)
                    FROM gold.dim_molecule dim_molecule
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM gold.fact_molecule_similarity fact
                        WHERE dim_molecule.chembl_id IN (
                            fact.source_chembl_id,
                            fact.target_chembl_id
                        )
                    )
                """,
            )

            assert_zero(
                value=dim_molecules_not_used,
                check_name='gold dimension contains only referred molecules',
            )

            referred_molecules_missing_from_dim = fetch_single_value(
                cursor=cursor,
                query="""
                    WITH referred_molecules AS (
                        SELECT DISTINCT
                            UNNEST(
                                ARRAY[source_chembl_id, target_chembl_id]
                            ) AS chembl_id
                        FROM gold.fact_molecule_similarity
                    )

                    SELECT COUNT(*)
                    FROM referred_molecules referred
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM gold.dim_molecule dim_molecule
                        WHERE dim_molecule.chembl_id = referred.chembl_id
                    )
                """,
            )

            assert_zero(
                value=referred_molecules_missing_from_dim,
                check_name='all referred molecules exist in gold dimension',
            )

            existing_views = fetch_single_value(
                cursor=cursor,
                query="""
                    SELECT COUNT(*)
                    FROM information_schema.views
                    WHERE table_schema = 'gold'
                      AND table_name = ANY(%s::TEXT[])
                """,
                parameters=(REQUIRED_GOLD_VIEWS,),
            )

            assert_equal(
                actual_value=existing_views,
                expected_value=len(REQUIRED_GOLD_VIEWS),
                check_name='required gold views exist',
            )

            for view_name in REQUIRED_GOLD_VIEWS:
                row_count = get_relation_row_count(
                    cursor=cursor,
                    schema_name='gold',
                    relation_name=view_name,
                )

                assert_positive(
                    value=row_count,
                    check_name=f'gold.{view_name} row count',
                )

            total_grouping_rows = fetch_single_value(
                cursor=cursor,
                query="""
                    SELECT COUNT(*)
                    FROM gold.vw_avg_similarity_grouping_sets
                    WHERE aggregation_level = 'TOTAL'
                      AND source_chembl_id = 'TOTAL'
                      AND source_aromatic_rings = 'TOTAL'
                      AND source_heavy_atoms = 'TOTAL'
                """,
            )

            assert_equal(
                actual_value=total_grouping_rows,
                expected_value=1,
                check_name='grouping sets total row',
            )

            pivot_column_count = fetch_single_value(
                cursor=cursor,
                query="""
                    SELECT COUNT(*)
                    FROM information_schema.columns
                    WHERE table_schema = 'gold'
                      AND table_name = 'vw_similarity_pivot_10_sources'
                """,
            )

            expected_pivot_columns = min(10, source_count) + 1

            assert_equal(
                actual_value=pivot_column_count,
                expected_value=expected_pivot_columns,
                check_name='pivot view column count',
            )

    finally:
        connection.close()

    results = {
        'gold_fact_rows': fact_rows,
        'gold_dim_rows': dim_rows,
        'gold_source_molecules': source_count,
    }

    logger.info('Gold quality checks passed: %s', results)

    return results
