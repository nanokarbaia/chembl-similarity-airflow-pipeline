"""Build gold data mart and reporting views for molecule similarities."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from lib.chembl.constants import (
    AWS_CONN_ID,
    CHEMBL_ID_PATTERN,
    DWH_CONN_ID,
    S3_BUCKET,
    TOP10_FILE_NAME,
    TOP10_S3_KEY,
)
from lib.utils.parsing import (
    normalize_chembl_id,
    parse_bool_value,
)
from lib.utils.s3 import (
    download_s3_file,
    get_s3_client,
)
from lib.utils.sql_files import execute_sql_file

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parents[2] / 'sql' / 'gold'

CHEMBL_ID_REGEX = re.compile(CHEMBL_ID_PATTERN)

REQUIRED_TOP10_COLUMNS = [
    'source_chembl_id',
    'target_chembl_id',
    'similarity_score',
    'similarity_rank',
    'has_duplicates_of_last_largest_score',
]

GOLD_SQL_FILES = {
    'drop_objects': '00_drop_gold_objects.sql',
    'create_fact': '01_create_fact_molecule_similarity.sql',
    'create_dim': '02_create_dim_molecule.sql',
    'create_views': '03_create_gold_views.sql',
}

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


@dataclass(frozen=True)
class GoldBuildResult:
    """Result metadata for gold data mart build."""

    fact_molecule_similarity_rows: int
    dim_molecule_rows: int

    def to_dict(self) -> dict[str, int]:
        """Convert result to dictionary for Airflow XCom."""
        return {
            'fact_molecule_similarity_rows': (
                self.fact_molecule_similarity_rows
            ),
            'dim_molecule_rows': self.dim_molecule_rows,
        }


@dataclass(frozen=True)
class GoldViewsResult:
    """Result metadata for gold view creation."""

    views_created: int
    pivot_source_ids: list[str]

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary for Airflow XCom."""
        return {
            'views_created': self.views_created,
            'pivot_source_ids': self.pivot_source_ids,
        }


def validate_required_top10_columns(dataframe: pd.DataFrame) -> None:
    """Validate that required top-N columns exist."""
    missing_columns = [
        column
        for column in REQUIRED_TOP10_COLUMNS
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(f'Missing required columns: {missing_columns}')


def validate_chembl_ids(dataframe: pd.DataFrame) -> None:
    """Validate source and target ChEMBL IDs."""
    invalid_chembl_ids = dataframe[
        (~dataframe['source_chembl_id'].str.fullmatch(CHEMBL_ID_PATTERN))
        | (~dataframe['target_chembl_id'].str.fullmatch(CHEMBL_ID_PATTERN))
    ]

    if not invalid_chembl_ids.empty:
        raise ValueError('Invalid ChEMBL IDs found in top-N dataframe.')


def validate_top10_content(dataframe: pd.DataFrame) -> None:
    """Validate business rules for top-N similarity data."""
    duplicated_pairs = dataframe.duplicated(
        subset=['source_chembl_id', 'target_chembl_id'],
    )

    if duplicated_pairs.any():
        raise ValueError('Duplicate source-target molecule pairs found.')

    self_matches = dataframe[
        dataframe['source_chembl_id'] == dataframe['target_chembl_id']
    ]

    if not self_matches.empty:
        raise ValueError('Top-N dataframe contains source-target self matches.')

    invalid_scores = (
        (dataframe['similarity_score'] < 0)
        | (dataframe['similarity_score'] > 1)
    )

    if invalid_scores.any():
        raise ValueError('Similarity score must be between 0 and 1.')

    invalid_ranks = dataframe['similarity_rank'] < 1

    if invalid_ranks.any():
        raise ValueError('Similarity rank must be at least 1.')

    duplicate_ranks = dataframe.duplicated(
        subset=['source_chembl_id', 'similarity_rank'],
    )

    if duplicate_ranks.any():
        raise ValueError('Duplicate similarity ranks found for one source.')

    invalid_score_order_sources = []
    invalid_rank_sequence_sources = []

    for source_chembl_id, source_rows in dataframe.groupby('source_chembl_id'):
        ordered_rows = source_rows.sort_values('similarity_rank')

        if not ordered_rows['similarity_score'].is_monotonic_decreasing:
            invalid_score_order_sources.append(source_chembl_id)

        expected_ranks = list(range(1, len(ordered_rows) + 1))
        actual_ranks = ordered_rows['similarity_rank'].tolist()

        if actual_ranks != expected_ranks:
            invalid_rank_sequence_sources.append(source_chembl_id)

    if invalid_score_order_sources:
        raise ValueError(
            'Top-N dataframe has invalid similarity score order for sources: '
            f'{invalid_score_order_sources}'
        )

    if invalid_rank_sequence_sources:
        raise ValueError(
            'Top-N dataframe has invalid rank sequence for sources: '
            f'{invalid_rank_sequence_sources}'
        )


def validate_top10_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Validate and clean top-N similarity dataframe."""
    validate_required_top10_columns(dataframe)

    if dataframe.empty:
        raise ValueError('Top-N similarity dataframe is empty.')

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
        result['has_duplicates_of_last_largest_score'].map(parse_bool_value)
    )

    if result[REQUIRED_TOP10_COLUMNS].isna().any().any():
        raise ValueError('Top-N similarity dataframe contains null values.')

    validate_chembl_ids(result)
    validate_top10_content(result)

    return result


def dataframe_to_fact_records(
    dataframe: pd.DataFrame,
) -> list[tuple[str, str, float, int, bool]]:
    """Convert dataframe rows to native Python tuples for PostgreSQL insert."""
    return [
        (
            str(row.source_chembl_id),
            str(row.target_chembl_id),
            float(row.similarity_score),
            int(row.similarity_rank),
            bool(row.has_duplicates_of_last_largest_score),
        )
        for row in dataframe.itertuples(index=False)
    ]


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
        if not CHEMBL_ID_REGEX.fullmatch(source_id)
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


class GoldDataMartBuilder:
    """Build gold dimension and fact tables from top-N S3 results."""

    def __init__(
        self,
        postgres_conn_id: str = DWH_CONN_ID,
        aws_conn_id: str = AWS_CONN_ID,
    ) -> None:
        self.postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.s3_client = get_s3_client(aws_conn_id=aws_conn_id)

    def download_top10_file(self, output_dir: Path) -> Path:
        """Download combined top-N parquet file from S3."""
        local_path = output_dir / TOP10_FILE_NAME

        download_s3_file(
            s3_client=self.s3_client,
            bucket_name=S3_BUCKET,
            key=TOP10_S3_KEY,
            local_path=local_path,
        )

        return local_path

    def read_fact_records_from_s3(self) -> list[tuple[str, str, float, int, bool]]:
        """Read and validate top-N records from S3."""
        with TemporaryDirectory(prefix='chembl_gold_') as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            top10_path = self.download_top10_file(output_dir=temp_dir)

            top10_dataframe = pd.read_parquet(top10_path)
            top10_dataframe = validate_top10_dataframe(top10_dataframe)

        return dataframe_to_fact_records(top10_dataframe)

    def insert_fact_records(
        self,
        cursor,
        fact_records: list[tuple[str, str, float, int, bool]],
    ) -> None:
        """Insert fact similarity records into gold table."""
        if not fact_records:
            raise ValueError('No fact records available for gold data mart.')

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

    def build(self) -> GoldBuildResult:
        """Build gold fact and dimension tables."""
        fact_records = self.read_fact_records_from_s3()
        connection = self.postgres_hook.get_conn()
        fact_count = 0
        dim_count = 0

        try:
            with connection:
                with connection.cursor() as cursor:
                    execute_sql_file(
                        cursor=cursor,
                        sql_dir=SQL_DIR,
                        file_name=GOLD_SQL_FILES['drop_objects'],
                    )

                    execute_sql_file(
                        cursor=cursor,
                        sql_dir=SQL_DIR,
                        file_name=GOLD_SQL_FILES['create_fact'],
                    )

                    self.insert_fact_records(
                        cursor=cursor,
                        fact_records=fact_records,
                    )

                    execute_sql_file(
                        cursor=cursor,
                        sql_dir=SQL_DIR,
                        file_name=GOLD_SQL_FILES['create_dim'],
                    )

                    cursor.execute(
                        'SELECT COUNT(*) FROM gold.fact_molecule_similarity'
                    )
                    fact_count = cursor.fetchone()[0]

                    cursor.execute('SELECT COUNT(*) FROM gold.dim_molecule')
                    dim_count = cursor.fetchone()[0]

        finally:
            connection.close()

        return GoldBuildResult(
            fact_molecule_similarity_rows=fact_count,
            dim_molecule_rows=dim_count,
        )


class GoldViewBuilder:
    """Create reporting views on top of the gold data mart."""

    def __init__(self, postgres_conn_id: str = DWH_CONN_ID) -> None:
        self.postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    @staticmethod
    def get_random_pivot_source_ids(cursor) -> list[str]:
        """Fetch random source IDs for the pivot view."""
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

        source_ids = [row[0] for row in cursor.fetchall()]

        if not source_ids:
            raise ValueError('No source molecule IDs available for pivot view.')

        return source_ids

    def create(self) -> GoldViewsResult:
        """Create required gold reporting views."""
        connection = self.postgres_hook.get_conn()
        pivot_source_ids: list[str] = []

        try:
            with connection:
                with connection.cursor() as cursor:
                    execute_sql_file(
                        cursor=cursor,
                        sql_dir=SQL_DIR,
                        file_name=GOLD_SQL_FILES['create_views'],
                    )

                    pivot_source_ids = self.get_random_pivot_source_ids(cursor)

                    logger.info(
                        'Creating pivot view for source molecules: %s',
                        pivot_source_ids,
                    )

                    cursor.execute(build_pivot_view_sql(pivot_source_ids))

        finally:
            connection.close()

        return GoldViewsResult(
            views_created=5,
            pivot_source_ids=pivot_source_ids,
        )


def build_gold_data_mart() -> dict[str, int]:
    """Build gold dimension and fact tables from top-N S3 results."""
    result = GoldDataMartBuilder().build().to_dict()

    logger.info('Gold data mart created: %s', result)

    return result


def create_gold_views() -> dict[str, Any]:
    """Create required reporting views on top of the gold data mart."""
    result = GoldViewBuilder().create().to_dict()

    logger.info('Gold views created: %s', result)

    return result
