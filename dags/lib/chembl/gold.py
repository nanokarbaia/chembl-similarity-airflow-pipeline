"""Build gold data mart and reporting views for molecule similarities."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from lib.chembl.constants import (
    AWS_CONN_ID,
    DWH_CONN_ID,
    S3_BUCKET,
    TOP10_S3_PREFIX,
)

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parents[2] / 'sql' / 'gold'

TOP10_FILE_NAME = 'top10_similar_molecules.parquet'
TOP10_S3_KEY = f'{TOP10_S3_PREFIX}/{TOP10_FILE_NAME}'

CHEMBL_ID_PATTERN = re.compile(r'^CHEMBL[0-9]+$')

REQUIRED_TOP10_COLUMNS = [
    'source_chembl_id',
    'target_chembl_id',
    'similarity_score',
    'similarity_rank',
    'has_duplicates_of_last_largest_score',
]

INSERT_FACT_SQL = """
INSERT INTO gold.fact_molecule_similarity (
    source_chembl_id,
    target_chembl_id,
    similarity_score,
    similarity_rank,
    has_duplicates_of_last_largest_score
)
VALUES %s
"""


def read_sql_file(file_name: str) -> str:
    """Read SQL file from the DAG SQL directory."""
    file_path = SQL_DIR / file_name

    if not file_path.exists():
        raise FileNotFoundError(f'SQL file not found: {file_path}')

    return file_path.read_text(encoding='utf-8')


def execute_sql_file(cursor, file_name: str) -> None:
    """Execute SQL from a file."""
    logger.info('Executing SQL file: %s', file_name)
    cursor.execute(read_sql_file(file_name))


def get_s3_client():
    """Create S3 client from Airflow connection."""
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    return s3_hook.get_conn()


def normalize_chembl_id(value: Any) -> str | None:
    """Normalize ChEMBL ID values."""
    if value is None:
        return None

    normalized_value = str(value).strip().upper()

    if not normalized_value or normalized_value in {'NAN', 'NONE', 'NULL'}:
        return None

    return normalized_value


def validate_top10_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Validate and clean top-N similarity dataframe."""
    missing_columns = [
        column
        for column in REQUIRED_TOP10_COLUMNS
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(f'Missing required columns: {missing_columns}')

    if dataframe.empty:
        raise ValueError('Top-10 similarity dataframe is empty.')

    result = dataframe[REQUIRED_TOP10_COLUMNS].copy()

    result['source_chembl_id'] = (
        result['source_chembl_id'].map(normalize_chembl_id)
    )
    result['target_chembl_id'] = (
        result['target_chembl_id'].map(normalize_chembl_id)
    )

    result['similarity_score'] = pd.to_numeric(
        result['similarity_score'],
        errors='raise',
    )

    result['similarity_rank'] = pd.to_numeric(
        result['similarity_rank'],
        errors='raise',
    ).astype(int)

    result['has_duplicates_of_last_largest_score'] = (
        result['has_duplicates_of_last_largest_score'].astype(bool)
    )

    if result[REQUIRED_TOP10_COLUMNS].isna().any().any():
        raise ValueError('Top-10 similarity dataframe contains null values.')

    invalid_chembl_ids = result[
        (~result['source_chembl_id'].str.match(CHEMBL_ID_PATTERN))
        | (~result['target_chembl_id'].str.match(CHEMBL_ID_PATTERN))
    ]

    if not invalid_chembl_ids.empty:
        raise ValueError('Invalid ChEMBL IDs found in top-10 dataframe.')

    duplicated_pairs = result.duplicated(
        subset=['source_chembl_id', 'target_chembl_id'],
    )

    if duplicated_pairs.any():
        raise ValueError('Duplicate source-target molecule pairs found.')

    invalid_scores = (
        (result['similarity_score'] < 0)
        | (result['similarity_score'] > 1)
    )

    if invalid_scores.any():
        raise ValueError('Similarity score must be between 0 and 1.')

    return result


def dataframe_to_fact_records(
    dataframe: pd.DataFrame,
) -> list[tuple[str, str, float, int, bool]]:
    """Convert dataframe rows to native Python tuples for PostgreSQL insert."""
    records = []

    for row in dataframe.itertuples(index=False):
        records.append(
            (
                str(row.source_chembl_id),
                str(row.target_chembl_id),
                float(row.similarity_score),
                int(row.similarity_rank),
                bool(row.has_duplicates_of_last_largest_score),
            )
        )

    return records


def download_top10_file(output_dir: Path) -> Path:
    """Download combined top-10 parquet file from S3."""
    s3_client = get_s3_client()
    local_path = output_dir / TOP10_FILE_NAME

    logger.info(
        'Downloading top-10 file from s3://%s/%s',
        S3_BUCKET,
        TOP10_S3_KEY,
    )

    s3_client.download_file(
        S3_BUCKET,
        TOP10_S3_KEY,
        str(local_path),
    )

    return local_path


def build_gold_data_mart() -> dict[str, int]:
    """Build gold dimension and fact tables from top-N S3 results."""
    postgres_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)

    with TemporaryDirectory(prefix='chembl_gold_') as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        top10_path = download_top10_file(output_dir=temp_dir)

        top10_dataframe = pd.read_parquet(top10_path)
        top10_dataframe = validate_top10_dataframe(top10_dataframe)
        fact_records = dataframe_to_fact_records(top10_dataframe)

    connection = postgres_hook.get_conn()

    try:
        with connection:
            with connection.cursor() as cursor:
                execute_sql_file(cursor, '00_drop_gold_objects.sql')
                execute_sql_file(cursor, '01_create_fact_molecule_similarity.sql')

                logger.info(
                    'Inserting %s row(s) into gold.fact_molecule_similarity.',
                    len(fact_records),
                )

                execute_values(
                    cursor,
                    INSERT_FACT_SQL,
                    fact_records,
                    page_size=1000,
                )

                execute_sql_file(cursor, '02_create_dim_molecule.sql')

                cursor.execute(
                    'SELECT COUNT(*) FROM gold.fact_molecule_similarity'
                )
                fact_count = cursor.fetchone()[0]

                cursor.execute('SELECT COUNT(*) FROM gold.dim_molecule')
                dim_count = cursor.fetchone()[0]

    finally:
        connection.close()

    result = {
        'fact_molecule_similarity_rows': fact_count,
        'dim_molecule_rows': dim_count,
    }

    logger.info('Gold data mart created: %s', result)

    return result


def quote_sql_identifier(value: str) -> str:
    """Safely quote SQL identifier."""
    return '"' + value.replace('"', '""') + '"'


def quote_sql_literal(value: str) -> str:
    """Safely quote SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def validate_pivot_source_ids(source_ids: list[str]) -> None:
    """Validate source molecule IDs used as pivot column names."""
    invalid_ids = [
        source_id
        for source_id in source_ids
        if not CHEMBL_ID_PATTERN.match(source_id)
    ]

    if invalid_ids:
        raise ValueError(f'Invalid source molecule IDs for pivot: {invalid_ids}')


def build_pivot_view_sql(source_ids: list[str]) -> str:
    """Build SQL for pivot view with selected source molecule columns."""
    if not source_ids:
        raise ValueError('No source molecule IDs available for pivot view.')

    validate_pivot_source_ids(source_ids)

    pivot_columns = []

    for source_id in source_ids:
        pivot_columns.append(
            '    MAX(CASE WHEN source_chembl_id = '
            f'{quote_sql_literal(source_id)} '
            f'THEN similarity_score END) AS {quote_sql_identifier(source_id)}'
        )

    pivot_columns_sql = ',\n'.join(pivot_columns)

    source_filter = ', '.join(
        quote_sql_literal(source_id)
        for source_id in source_ids
    )

    return f"""
CREATE OR REPLACE VIEW gold.vw_similarity_pivot_10_sources AS
SELECT
    target_chembl_id,
{pivot_columns_sql}
FROM gold.fact_molecule_similarity
WHERE source_chembl_id IN ({source_filter})
GROUP BY target_chembl_id;
"""


def create_gold_views() -> dict[str, Any]:
    """Create required reporting views on top of the gold data mart."""
    postgres_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    connection = postgres_hook.get_conn()
    pivot_source_ids: list[str] = []

    try:
        with connection:
            with connection.cursor() as cursor:
                execute_sql_file(cursor, '03_create_gold_views.sql')

                cursor.execute(
                    """
                    SELECT source_chembl_id
                    FROM (
                        SELECT DISTINCT source_chembl_id
                        FROM gold.fact_molecule_similarity
                    ) source_molecules
                    ORDER BY RANDOM()
                    LIMIT 10
                    """
                )

                pivot_source_ids = [row[0] for row in cursor.fetchall()]

                logger.info(
                    'Creating pivot view for source molecules: %s',
                    pivot_source_ids,
                )

                cursor.execute(build_pivot_view_sql(pivot_source_ids))

    finally:
        connection.close()

    result = {
        'views_created': 5,
        'pivot_source_ids': pivot_source_ids,
    }

    logger.info('Gold views created: %s', result)

    return result
