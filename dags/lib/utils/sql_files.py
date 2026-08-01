"""Reusable helpers for executing SQL files."""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def read_sql_file(sql_dir: Path, file_name: str) -> str:
    """Read SQL file from a configured SQL directory."""
    file_path = sql_dir / file_name

    if not file_path.is_file():
        raise FileNotFoundError(f'SQL file not found: {file_path}')

    return file_path.read_text(encoding='utf-8')


def execute_sql_file(cursor, sql_dir: Path, file_name: str) -> None:
    """Execute SQL from a file."""
    logger.info('Executing SQL file: %s', file_name)

    sql_text = read_sql_file(
        sql_dir=sql_dir,
        file_name=file_name,
    )

    cursor.execute(sql_text)
