"""Prepare cleaned molecule data in the silver DWH layer."""

from __future__ import annotations

import logging
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook

from lib.chembl.constants import DWH_CONN_ID
from lib.utils.data_quality import get_relation_row_count
from lib.utils.sql_files import execute_sql_file

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parents[2] / 'sql' / 'silver'
PREPARE_SILVER_SQL_FILE = '01_prepare_silver_molecules.sql'


class SilverMoleculePreparer:
    """Prepare cleaned molecule data in the silver DWH layer."""

    def __init__(self, postgres_conn_id: str = DWH_CONN_ID) -> None:
        self.postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    def prepare(self) -> int:
        """Create silver.molecules from bronze ChEMBL tables."""
        logger.info('Preparing silver.molecules table.')

        connection = self.postgres_hook.get_conn()
        row_count = 0

        try:
            with connection:
                with connection.cursor() as cursor:
                    execute_sql_file(
                        cursor=cursor,
                        sql_dir=SQL_DIR,
                        file_name=PREPARE_SILVER_SQL_FILE,
                    )

                    row_count = get_relation_row_count(
                        cursor=cursor,
                        schema_name='silver',
                        relation_name='molecules',
                    )

        finally:
            connection.close()

        if row_count == 0:
            raise ValueError('silver.molecules is empty after preparation.')

        logger.info('silver.molecules prepared. Row count: %s', row_count)

        return row_count


def prepare_silver_molecules() -> int:
    """Create silver.molecules from bronze ChEMBL tables."""
    return SilverMoleculePreparer().prepare()
