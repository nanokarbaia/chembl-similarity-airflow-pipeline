"""Load required ChEMBL SQLite tables into the PostgreSQL bronze layer."""

from __future__ import annotations

import csv
import io
import logging
import os
import sqlite3
import tarfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

from lib.chembl.bronze_schema import create_standard_bronze_tables
from lib.chembl.constants import (
    BRONZE_SCHEMA,
    CHEMBL_CACHE_DIR,
    CHEMBL_FTP_BASE_URL,
    DEFAULT_CHEMBL_VERSION,
    DEFAULT_LOAD_BATCH_SIZE,
    DWH_CONN_ID,
)

logger = logging.getLogger(__name__)


def get_chembl_version(chembl_version: str | None = None) -> str:
    """Return ChEMBL version used for full ingestion."""
    return str(chembl_version or DEFAULT_CHEMBL_VERSION).replace('chembl_', '')


def get_chembl_sqlite_url(chembl_version: str) -> str:
    """Build ChEMBL SQLite archive URL."""
    return (
        f'{CHEMBL_FTP_BASE_URL}/chembl_{chembl_version}/'
        f'chembl_{chembl_version}_sqlite.tar.gz'
    )


def download_with_resume(
    url: str,
    destination_path: Path,
    retries: int = 5,
    chunk_size: int = 1024 * 1024,
) -> Path:
    """Download a large file with resume support."""
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    partial_path = destination_path.with_suffix(destination_path.suffix + '.part')

    if destination_path.exists() and destination_path.stat().st_size > 0:
        logger.info('Archive already exists: %s', destination_path)
        return destination_path

    for attempt in range(1, retries + 1):
        existing_size = partial_path.stat().st_size if partial_path.exists() else 0
        headers = {}

        if existing_size > 0:
            headers['Range'] = f'bytes={existing_size}-'
            logger.info(
                'Resuming download from byte %s. Attempt %s/%s',
                existing_size,
                attempt,
                retries,
            )
        else:
            logger.info('Starting download. Attempt %s/%s', attempt, retries)

        try:
            with requests.get(
                url,
                headers=headers,
                stream=True,
                timeout=(30, 300),
            ) as response:
                response.raise_for_status()

                mode = 'ab' if response.status_code == 206 else 'wb'

                if response.status_code == 200 and existing_size > 0:
                    logger.warning(
                        'Server did not resume download. Restarting from zero.'
                    )

                bytes_written = existing_size if mode == 'ab' else 0
                last_logged_size = bytes_written

                with partial_path.open(mode) as file:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if not chunk:
                            continue

                        file.write(chunk)
                        bytes_written += len(chunk)

                        if bytes_written - last_logged_size >= 100 * 1024 * 1024:
                            logger.info(
                                'Downloaded %.2f GB',
                                bytes_written / 1024 / 1024 / 1024,
                            )
                            last_logged_size = bytes_written

            partial_path.rename(destination_path)
            logger.info('Download finished: %s', destination_path)

            return destination_path

        except Exception as exc:
            logger.warning(
                'Download attempt %s/%s failed: %s',
                attempt,
                retries,
                exc,
            )

            if attempt == retries:
                raise

            time.sleep(30 * attempt)

    raise RuntimeError(f'Could not download file from {url}')


def find_extracted_sqlite_file(extract_dir: Path) -> Path:
    """Find SQLite database file after archive extraction."""
    candidates = [
        *extract_dir.rglob('*.db'),
        *extract_dir.rglob('*.sqlite'),
        *extract_dir.rglob('*.sqlite3'),
    ]

    if not candidates:
        raise FileNotFoundError(f'No SQLite database found under {extract_dir}')

    return candidates[0]


def extract_sqlite_archive(archive_path: Path, chembl_version: str) -> Path:
    """Extract ChEMBL SQLite archive and return SQLite database path."""
    extract_dir = Path(CHEMBL_CACHE_DIR) / f'chembl_{chembl_version}_sqlite'

    if extract_dir.exists():
        sqlite_path = find_extracted_sqlite_file(extract_dir)
        logger.info('SQLite database already extracted: %s', sqlite_path)
        return sqlite_path

    extract_dir.mkdir(parents=True, exist_ok=True)

    try:
        with tarfile.open(archive_path, 'r:gz') as archive:
            archive.extractall(extract_dir)
    except tarfile.TarError:
        logger.exception('Archive is corrupted. Deleting archive: %s', archive_path)
        archive_path.unlink(missing_ok=True)
        raise

    sqlite_path = find_extracted_sqlite_file(extract_dir)
    logger.info('SQLite database extracted: %s', sqlite_path)

    return sqlite_path


def ensure_chembl_sqlite(chembl_version: str | None = None) -> tuple[str, Path]:
    """Ensure ChEMBL SQLite database is available locally."""
    resolved_version = get_chembl_version(chembl_version)
    cache_dir = Path(CHEMBL_CACHE_DIR)
    archive_path = cache_dir / f'chembl_{resolved_version}_sqlite.tar.gz'
    url = get_chembl_sqlite_url(resolved_version)

    logger.info('Using ChEMBL version: %s', resolved_version)
    logger.info('ChEMBL SQLite URL: %s', url)
    logger.info('ChEMBL cache directory: %s', cache_dir)

    downloaded_archive_path = download_with_resume(
        url=url,
        destination_path=archive_path,
    )

    sqlite_path = extract_sqlite_archive(
        archive_path=downloaded_archive_path,
        chembl_version=resolved_version,
    )

    return resolved_version, sqlite_path


def get_sqlite_columns(connection: sqlite3.Connection, table_name: str) -> set[str]:
    """Return available columns for a SQLite table."""
    cursor = connection.execute(f'PRAGMA table_info("{table_name}")')
    rows = cursor.fetchall()

    if not rows:
        raise ValueError(f'Table {table_name} was not found in SQLite database.')

    return {row[1] for row in rows}


def column_or_null(
    available_columns: set[str],
    column_name: str,
    alias: str | None = None,
    table_alias: str | None = None,
) -> str:
    """Return SQLite column expression or NULL if the column does not exist."""
    output_alias = alias or column_name

    if column_name not in available_columns:
        return f'NULL AS "{output_alias}"'

    if table_alias:
        return f'{table_alias}."{column_name}" AS "{output_alias}"'

    return f'"{column_name}" AS "{output_alias}"'


def copy_rows(
    connection,
    table_name: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
) -> int:
    """Copy rows into PostgreSQL bronze table."""
    if not rows:
        return 0

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(rows)
    output.seek(0)

    column_list = ', '.join(f'"{column}"' for column in columns)

    copy_statement = f'''
        COPY {BRONZE_SCHEMA}.{table_name} ({column_list})
        FROM STDIN WITH (FORMAT CSV)
    '''

    with connection.cursor() as cursor:
        cursor.copy_expert(copy_statement, output)

    connection.commit()

    return len(rows)


def stream_sqlite_query_to_postgres(
    sqlite_connection: sqlite3.Connection,
    postgres_connection,
    query: str,
    table_name: str,
    columns: list[str],
    batch_size: int = DEFAULT_LOAD_BATCH_SIZE,
) -> int:
    """Stream SQLite query result into PostgreSQL."""
    cursor = sqlite_connection.execute(query)
    total_rows = 0

    while True:
        rows = cursor.fetchmany(batch_size)

        if not rows:
            break

        inserted_rows = copy_rows(
            connection=postgres_connection,
            table_name=table_name,
            columns=columns,
            rows=rows,
        )

        total_rows += inserted_rows
        logger.info('Loaded %s rows into bronze.%s', total_rows, table_name)

    return total_rows


def load_chembl_id_lookup(
    sqlite_connection: sqlite3.Connection,
    postgres_connection,
    chembl_version: str,
    record_limit: int | None,
    loaded_at: datetime,
) -> int:
    """Load chembl_id_lookup from SQLite into bronze."""
    available_columns = get_sqlite_columns(sqlite_connection, 'chembl_id_lookup')

    query = f'''
        SELECT
            {column_or_null(available_columns, 'chembl_id')},
            {column_or_null(available_columns, 'entity_type')},
            {column_or_null(available_columns, 'status')},
            {column_or_null(available_columns, 'resource_url')},
            NULL AS raw_record,
            'chembl_sqlite_dump' AS source_system,
            '{chembl_version}' AS source_chembl_version,
            '{loaded_at.isoformat()}' AS loaded_at
        FROM chembl_id_lookup
    '''

    if record_limit is not None:
        query = f'{query} LIMIT {int(record_limit)}'

    return stream_sqlite_query_to_postgres(
        sqlite_connection=sqlite_connection,
        postgres_connection=postgres_connection,
        query=query,
        table_name='chembl_id_lookup',
        columns=[
            'chembl_id',
            'entity_type',
            'status',
            'resource_url',
            'raw_record',
            'source_system',
            'source_chembl_version',
            'loaded_at',
        ],
    )


def load_molecule_dictionary(
    sqlite_connection: sqlite3.Connection,
    postgres_connection,
    chembl_version: str,
    record_limit: int | None,
    loaded_at: datetime,
) -> int:
    """Load molecule_dictionary from SQLite into bronze."""
    available_columns = get_sqlite_columns(sqlite_connection, 'molecule_dictionary')

    query = f'''
        SELECT
            {column_or_null(available_columns, 'molregno')},
            {column_or_null(available_columns, 'chembl_id')},
            {column_or_null(available_columns, 'molecule_type')},
            {column_or_null(available_columns, 'pref_name')},
            {column_or_null(available_columns, 'max_phase')},
            {column_or_null(available_columns, 'therapeutic_flag')},
            NULL AS raw_record,
            'chembl_sqlite_dump' AS source_system,
            '{chembl_version}' AS source_chembl_version,
            '{loaded_at.isoformat()}' AS loaded_at
        FROM molecule_dictionary
    '''

    if record_limit is not None:
        query = f'{query} LIMIT {int(record_limit)}'

    return stream_sqlite_query_to_postgres(
        sqlite_connection=sqlite_connection,
        postgres_connection=postgres_connection,
        query=query,
        table_name='molecule_dictionary',
        columns=[
            'molregno',
            'chembl_id',
            'molecule_type',
            'pref_name',
            'max_phase',
            'therapeutic_flag',
            'raw_record',
            'source_system',
            'source_chembl_version',
            'loaded_at',
        ],
    )


def load_compound_properties(
    sqlite_connection: sqlite3.Connection,
    postgres_connection,
    chembl_version: str,
    record_limit: int | None,
    loaded_at: datetime,
) -> int:
    """Load compound_properties from SQLite into bronze."""
    cp_columns = get_sqlite_columns(sqlite_connection, 'compound_properties')

    query = f'''
        SELECT
            cp.molregno AS molregno,
            md.chembl_id AS chembl_id,
            {column_or_null(cp_columns, 'mw_freebase', table_alias='cp')},
            {column_or_null(cp_columns, 'alogp', table_alias='cp')},
            {column_or_null(cp_columns, 'psa', table_alias='cp')},
            {column_or_null(cp_columns, 'cx_logp', table_alias='cp')},
            {column_or_null(cp_columns, 'molecular_species', table_alias='cp')},
            {column_or_null(cp_columns, 'full_mwt', table_alias='cp')},
            {column_or_null(cp_columns, 'aromatic_rings', table_alias='cp')},
            {column_or_null(cp_columns, 'heavy_atoms', table_alias='cp')},
            NULL AS raw_record,
            'chembl_sqlite_dump' AS source_system,
            '{chembl_version}' AS source_chembl_version,
            '{loaded_at.isoformat()}' AS loaded_at
        FROM compound_properties cp
        JOIN molecule_dictionary md
            ON cp.molregno = md.molregno
    '''

    if record_limit is not None:
        query = f'{query} LIMIT {int(record_limit)}'

    return stream_sqlite_query_to_postgres(
        sqlite_connection=sqlite_connection,
        postgres_connection=postgres_connection,
        query=query,
        table_name='compound_properties',
        columns=[
            'molregno',
            'chembl_id',
            'mw_freebase',
            'alogp',
            'psa',
            'cx_logp',
            'molecular_species',
            'full_mwt',
            'aromatic_rings',
            'heavy_atoms',
            'raw_record',
            'source_system',
            'source_chembl_version',
            'loaded_at',
        ],
    )


def load_compound_structures(
    sqlite_connection: sqlite3.Connection,
    postgres_connection,
    chembl_version: str,
    record_limit: int | None,
    loaded_at: datetime,
) -> int:
    """Load compound_structures from SQLite into bronze."""
    cs_columns = get_sqlite_columns(sqlite_connection, 'compound_structures')

    query = f'''
        SELECT
            cs.molregno AS molregno,
            md.chembl_id AS chembl_id,
            {column_or_null(cs_columns, 'canonical_smiles', table_alias='cs')},
            {column_or_null(cs_columns, 'standard_inchi', table_alias='cs')},
            {column_or_null(cs_columns, 'standard_inchi_key', table_alias='cs')},
            NULL AS raw_record,
            'chembl_sqlite_dump' AS source_system,
            '{chembl_version}' AS source_chembl_version,
            '{loaded_at.isoformat()}' AS loaded_at
        FROM compound_structures cs
        JOIN molecule_dictionary md
            ON cs.molregno = md.molregno
    '''

    if record_limit is not None:
        query = f'{query} LIMIT {int(record_limit)}'

    return stream_sqlite_query_to_postgres(
        sqlite_connection=sqlite_connection,
        postgres_connection=postgres_connection,
        query=query,
        table_name='compound_structures',
        columns=[
            'molregno',
            'chembl_id',
            'canonical_smiles',
            'standard_inchi',
            'standard_inchi_key',
            'raw_record',
            'source_system',
            'source_chembl_version',
            'loaded_at',
        ],
    )


def load_required_chembl_tables_to_bronze(
    chembl_version: str | None = None,
    record_limit: int | None = None,
) -> dict[str, int]:
    """Load required ChEMBL SQLite tables into PostgreSQL bronze."""
    resolved_version, sqlite_path = ensure_chembl_sqlite(chembl_version)

    postgres_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    postgres_connection = postgres_hook.get_conn()
    loaded_at = datetime.now(timezone.utc)

    try:
        create_standard_bronze_tables(postgres_connection)

        with sqlite3.connect(sqlite_path) as sqlite_connection:
            result = {
                'chembl_id_lookup': load_chembl_id_lookup(
                    sqlite_connection=sqlite_connection,
                    postgres_connection=postgres_connection,
                    chembl_version=resolved_version,
                    record_limit=record_limit,
                    loaded_at=loaded_at,
                ),
                'molecule_dictionary': load_molecule_dictionary(
                    sqlite_connection=sqlite_connection,
                    postgres_connection=postgres_connection,
                    chembl_version=resolved_version,
                    record_limit=record_limit,
                    loaded_at=loaded_at,
                ),
                'compound_properties': load_compound_properties(
                    sqlite_connection=sqlite_connection,
                    postgres_connection=postgres_connection,
                    chembl_version=resolved_version,
                    record_limit=record_limit,
                    loaded_at=loaded_at,
                ),
                'compound_structures': load_compound_structures(
                    sqlite_connection=sqlite_connection,
                    postgres_connection=postgres_connection,
                    chembl_version=resolved_version,
                    record_limit=record_limit,
                    loaded_at=loaded_at,
                ),
            }

        logger.info('Finished full ChEMBL bronze load: %s', result)

        return result

    finally:
        postgres_connection.close()
