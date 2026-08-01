"""Bronze table definitions for ChEMBL ingestion."""

from __future__ import annotations

import logging
from pathlib import Path

from lib.utils.sql_files import execute_sql_file

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parents[2] / 'sql' / 'bronze'
CREATE_BRONZE_TABLES_SQL_FILE = '00_create_bronze_tables.sql'


def create_standard_bronze_tables(connection) -> None:
    """Create standardized bronze tables for API and full dump ingestion."""
    logger.info('Creating standardized bronze ChEMBL tables.')

    with connection:
        with connection.cursor() as cursor:
            execute_sql_file(
                cursor=cursor,
                sql_dir=SQL_DIR,
                file_name=CREATE_BRONZE_TABLES_SQL_FILE,
            )

    logger.info('Bronze ChEMBL tables created.')
