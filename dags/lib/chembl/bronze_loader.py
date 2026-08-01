"""Load required ChEMBL SQLite tables into the PostgreSQL bronze layer."""

from __future__ import annotations

import csv
import io
import logging
import shutil
import sqlite3
import tarfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql

from lib.chembl.bronze_schema import create_standard_bronze_tables
from lib.chembl.constants import (
    BRONZE_SCHEMA,
    CHEMBL_CACHE_DIR,
    CHEMBL_FTP_BASE_URL,
    CHEMBL_SQLITE_SOURCE_SYSTEM,
    DEFAULT_CHEMBL_VERSION,
    DEFAULT_LOAD_BATCH_SIZE,
    DOWNLOAD_BACKOFF_SECONDS,
    DOWNLOAD_CHUNK_SIZE_BYTES,
    DOWNLOAD_CONNECT_TIMEOUT_SECONDS,
    DOWNLOAD_PROGRESS_STEP_BYTES,
    DOWNLOAD_READ_TIMEOUT_SECONDS,
    DOWNLOAD_RETRIES,
    DWH_CONN_ID,
)
from lib.utils.parsing import normalize_text, parse_positive_int

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChEMBLLoadContext:
    """Runtime context for a ChEMBL bronze load."""

    chembl_version: str
    loaded_at: datetime
    record_limit: int | None


def get_chembl_version(chembl_version: str | None = None) -> str:
    """Return ChEMBL version used for full ingestion."""
    normalized_version = normalize_text(chembl_version)

    if normalized_version is None:
        normalized_version = DEFAULT_CHEMBL_VERSION

    normalized_version = normalized_version.lower().replace('chembl_', '').strip()

    if not normalized_version:
        raise ValueError('ChEMBL version cannot be empty.')

    return normalized_version


def get_chembl_sqlite_url(chembl_version: str) -> str:
    """Build ChEMBL SQLite archive URL."""
    return (
        f'{CHEMBL_FTP_BASE_URL}/chembl_{chembl_version}/'
        f'chembl_{chembl_version}_sqlite.tar.gz'
    )


def validate_record_limit(record_limit: int | None) -> int | None:
    """Validate optional record limit."""
    if record_limit is None:
        return None

    return parse_positive_int(
        value=record_limit,
        parameter_name='record_limit',
    )


def quote_sqlite_identifier(identifier: str) -> str:
    """Quote SQLite identifier safely."""
    return '"' + identifier.replace('"', '""') + '"'


def download_with_resume(
    url: str,
    destination_path: Path,
    retries: int = DOWNLOAD_RETRIES,
    chunk_size: int = DOWNLOAD_CHUNK_SIZE_BYTES,
) -> Path:
    """Download a large file with resume support."""
    retries = parse_positive_int(retries, 'retries')
    chunk_size = parse_positive_int(chunk_size, 'chunk_size')

    destination_path.parent.mkdir(parents=True, exist_ok=True)
    partial_path = destination_path.with_suffix(f'{destination_path.suffix}.part')

    if destination_path.exists() and destination_path.stat().st_size > 0:
        logger.info('Archive already exists: %s', destination_path)
        return destination_path

    with requests.Session() as session:
        for attempt in range(1, retries + 1):
            existing_size = (
                partial_path.stat().st_size
                if partial_path.exists()
                else 0
            )
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
                with session.get(
                    url,
                    headers=headers,
                    stream=True,
                    timeout=(
                        DOWNLOAD_CONNECT_TIMEOUT_SECONDS,
                        DOWNLOAD_READ_TIMEOUT_SECONDS,
                    ),
                ) as response:
                    response.raise_for_status()

                    mode = 'ab' if response.status_code == 206 else 'wb'

                    if response.status_code == 200 and existing_size > 0:
                        logger.warning(
                            'Server did not resume download. '
                            'Restarting from zero.'
                        )

                    bytes_written = existing_size if mode == 'ab' else 0
                    last_logged_size = bytes_written

                    with partial_path.open(mode) as file:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            if not chunk:
                                continue

                            file.write(chunk)
                            bytes_written += len(chunk)

                            if (
                                bytes_written - last_logged_size
                                >= DOWNLOAD_PROGRESS_STEP_BYTES
                            ):
                                logger.info(
                                    'Downloaded %.2f GB',
                                    bytes_written / 1024 / 1024 / 1024,
                                )
                                last_logged_size = bytes_written

                partial_path.rename(destination_path)
                logger.info('Download finished: %s', destination_path)

                return destination_path

            except (requests.RequestException, OSError) as exc:
                logger.warning(
                    'Download attempt %s/%s failed: %s',
                    attempt,
                    retries,
                    exc,
                )

                if attempt == retries:
                    raise

                time.sleep(DOWNLOAD_BACKOFF_SECONDS * attempt)

    raise RuntimeError(f'Could not download file from {url}')


def find_extracted_sqlite_file(extract_dir: Path) -> Path:
    """Find SQLite database file after archive extraction."""
    candidates = sorted(
        [
            *extract_dir.rglob('*.db'),
            *extract_dir.rglob('*.sqlite'),
            *extract_dir.rglob('*.sqlite3'),
        ]
    )

    if not candidates:
        raise FileNotFoundError(f'No SQLite database found under {extract_dir}')

    return candidates[0]


def validate_tar_member_path(extract_dir: Path, member: tarfile.TarInfo) -> None:
    """Validate that tar member will be extracted safely."""
    extract_dir_path = extract_dir.resolve()
    member_path = (extract_dir / member.name).resolve()

    if not member_path.is_relative_to(extract_dir_path):
        raise ValueError(
            f'Unsafe path detected in ChEMBL archive: {member.name}'
        )

    if member.issym() or member.islnk():
        raise ValueError(
            f'Unsafe link detected in ChEMBL archive: {member.name}'
        )

    if member.isdev():
        raise ValueError(
            f'Unsafe device file detected in ChEMBL archive: {member.name}'
        )


def safe_extract_tar_archive(archive_path: Path, extract_dir: Path) -> None:
    """Safely extract a tar archive into a target directory."""
    with tarfile.open(archive_path, 'r:gz') as archive:
        for member in archive.getmembers():
            validate_tar_member_path(
                extract_dir=extract_dir,
                member=member,
            )

        archive.extractall(extract_dir)


def extract_sqlite_archive(archive_path: Path, chembl_version: str) -> Path:
    """Extract ChEMBL SQLite archive and return SQLite database path."""
    extract_dir = Path(CHEMBL_CACHE_DIR) / f'chembl_{chembl_version}_sqlite'

    if extract_dir.exists():
        try:
            sqlite_path = find_extracted_sqlite_file(extract_dir)
            logger.info('SQLite database already extracted: %s', sqlite_path)
            return sqlite_path
        except FileNotFoundError:
            logger.warning(
                'Existing extraction directory has no SQLite file. '
                'Re-extracting: %s',
                extract_dir,
            )
            shutil.rmtree(extract_dir)

    extract_dir.mkdir(parents=True, exist_ok=True)

    try:
        safe_extract_tar_archive(
            archive_path=archive_path,
            extract_dir=extract_dir,
        )
    except (tarfile.TarError, ValueError):
        logger.exception('Archive extraction failed: %s', archive_path)
        archive_path.unlink(missing_ok=True)
        shutil.rmtree(extract_dir, ignore_errors=True)
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
    quoted_table_name = quote_sqlite_identifier(table_name)
    cursor = connection.execute(f'PRAGMA table_info({quoted_table_name})')
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
    """Return SQLite column expression or NULL if column does not exist."""
    output_alias = alias or column_name
    quoted_alias = quote_sqlite_identifier(output_alias)

    if column_name not in available_columns:
        return f'NULL AS {quoted_alias}'

    quoted_column = quote_sqlite_identifier(column_name)

    if table_alias:
        return f'{table_alias}.{quoted_column} AS {quoted_alias}'

    return f'{quoted_column} AS {quoted_alias}'


def copy_rows(
    connection,
    table_name: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
) -> int:
    """Copy rows into a PostgreSQL bronze table."""
    if not rows:
        return 0

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(rows)
    output.seek(0)

    with connection.cursor() as cursor:
        copy_statement = sql.SQL(
            'COPY {}.{} ({}) FROM STDIN WITH (FORMAT CSV)'
        ).format(
            sql.Identifier(BRONZE_SCHEMA),
            sql.Identifier(table_name),
            sql.SQL(', ').join(sql.Identifier(column) for column in columns),
        )

        cursor.copy_expert(copy_statement.as_string(cursor), output)

    connection.commit()

    return len(rows)


def stream_sqlite_query_to_postgres(
    sqlite_connection: sqlite3.Connection,
    postgres_connection,
    query: str,
    query_parameters: tuple[Any, ...],
    table_name: str,
    columns: list[str],
    batch_size: int = DEFAULT_LOAD_BATCH_SIZE,
) -> int:
    """Stream SQLite query result into PostgreSQL."""
    batch_size = parse_positive_int(batch_size, 'batch_size')

    cursor = sqlite_connection.execute(query, query_parameters)
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


def add_optional_limit(
    query: str,
    query_parameters: list[Any],
    record_limit: int | None,
) -> tuple[str, tuple[Any, ...]]:
    """Add optional LIMIT clause to a SQLite query."""
    if record_limit is None:
        return query, tuple(query_parameters)

    query = f'{query}\nLIMIT ?'
    query_parameters.append(record_limit)

    return query, tuple(query_parameters)


def load_chembl_id_lookup(
    sqlite_connection: sqlite3.Connection,
    postgres_connection,
    context: ChEMBLLoadContext,
) -> int:
    """Load chembl_id_lookup from SQLite into bronze."""
    available_columns = get_sqlite_columns(
        sqlite_connection,
        'chembl_id_lookup',
    )

    query = f'''
        SELECT
            {column_or_null(available_columns, 'chembl_id')},
            {column_or_null(available_columns, 'entity_type')},
            {column_or_null(available_columns, 'status')},
            {column_or_null(available_columns, 'resource_url')},
            NULL AS raw_record,
            ? AS source_system,
            ? AS source_chembl_version,
            ? AS loaded_at
        FROM chembl_id_lookup
    '''

    query, query_parameters = add_optional_limit(
        query=query,
        query_parameters=[
            CHEMBL_SQLITE_SOURCE_SYSTEM,
            context.chembl_version,
            context.loaded_at.isoformat(),
        ],
        record_limit=context.record_limit,
    )

    return stream_sqlite_query_to_postgres(
        sqlite_connection=sqlite_connection,
        postgres_connection=postgres_connection,
        query=query,
        query_parameters=query_parameters,
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
    context: ChEMBLLoadContext,
) -> int:
    """Load molecule_dictionary from SQLite into bronze."""
    available_columns = get_sqlite_columns(
        sqlite_connection,
        'molecule_dictionary',
    )

    query = f'''
        SELECT
            {column_or_null(available_columns, 'molregno')},
            {column_or_null(available_columns, 'chembl_id')},
            {column_or_null(available_columns, 'molecule_type')},
            {column_or_null(available_columns, 'pref_name')},
            {column_or_null(available_columns, 'max_phase')},
            {column_or_null(available_columns, 'therapeutic_flag')},
            NULL AS raw_record,
            ? AS source_system,
            ? AS source_chembl_version,
            ? AS loaded_at
        FROM molecule_dictionary
    '''

    query, query_parameters = add_optional_limit(
        query=query,
        query_parameters=[
            CHEMBL_SQLITE_SOURCE_SYSTEM,
            context.chembl_version,
            context.loaded_at.isoformat(),
        ],
        record_limit=context.record_limit,
    )

    return stream_sqlite_query_to_postgres(
        sqlite_connection=sqlite_connection,
        postgres_connection=postgres_connection,
        query=query,
        query_parameters=query_parameters,
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
    context: ChEMBLLoadContext,
) -> int:
    """Load compound_properties from SQLite into bronze."""
    cp_columns = get_sqlite_columns(
        sqlite_connection,
        'compound_properties',
    )

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
            ? AS source_system,
            ? AS source_chembl_version,
            ? AS loaded_at
        FROM compound_properties cp
        JOIN molecule_dictionary md
            ON cp.molregno = md.molregno
    '''

    query, query_parameters = add_optional_limit(
        query=query,
        query_parameters=[
            CHEMBL_SQLITE_SOURCE_SYSTEM,
            context.chembl_version,
            context.loaded_at.isoformat(),
        ],
        record_limit=context.record_limit,
    )

    return stream_sqlite_query_to_postgres(
        sqlite_connection=sqlite_connection,
        postgres_connection=postgres_connection,
        query=query,
        query_parameters=query_parameters,
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
    context: ChEMBLLoadContext,
) -> int:
    """Load compound_structures from SQLite into bronze."""
    cs_columns = get_sqlite_columns(
        sqlite_connection,
        'compound_structures',
    )

    query = f'''
        SELECT
            cs.molregno AS molregno,
            md.chembl_id AS chembl_id,
            {column_or_null(cs_columns, 'canonical_smiles', table_alias='cs')},
            {column_or_null(cs_columns, 'standard_inchi', table_alias='cs')},
            {column_or_null(cs_columns, 'standard_inchi_key', table_alias='cs')},
            NULL AS raw_record,
            ? AS source_system,
            ? AS source_chembl_version,
            ? AS loaded_at
        FROM compound_structures cs
        JOIN molecule_dictionary md
            ON cs.molregno = md.molregno
    '''

    query, query_parameters = add_optional_limit(
        query=query,
        query_parameters=[
            CHEMBL_SQLITE_SOURCE_SYSTEM,
            context.chembl_version,
            context.loaded_at.isoformat(),
        ],
        record_limit=context.record_limit,
    )

    return stream_sqlite_query_to_postgres(
        sqlite_connection=sqlite_connection,
        postgres_connection=postgres_connection,
        query=query,
        query_parameters=query_parameters,
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


class ChEMBLBronzeLoader:
    """Load required ChEMBL SQLite tables into the bronze DWH layer."""

    def __init__(
        self,
        postgres_connection,
        sqlite_connection: sqlite3.Connection,
        context: ChEMBLLoadContext,
    ) -> None:
        self.postgres_connection = postgres_connection
        self.sqlite_connection = sqlite_connection
        self.context = context

    def load(self) -> dict[str, int]:
        """Load all required ChEMBL tables."""
        return {
            'chembl_id_lookup': load_chembl_id_lookup(
                sqlite_connection=self.sqlite_connection,
                postgres_connection=self.postgres_connection,
                context=self.context,
            ),
            'molecule_dictionary': load_molecule_dictionary(
                sqlite_connection=self.sqlite_connection,
                postgres_connection=self.postgres_connection,
                context=self.context,
            ),
            'compound_properties': load_compound_properties(
                sqlite_connection=self.sqlite_connection,
                postgres_connection=self.postgres_connection,
                context=self.context,
            ),
            'compound_structures': load_compound_structures(
                sqlite_connection=self.sqlite_connection,
                postgres_connection=self.postgres_connection,
                context=self.context,
            ),
        }


def load_required_chembl_tables_to_bronze(
    chembl_version: str | None = None,
    record_limit: int | None = None,
) -> dict[str, int]:
    """Load required ChEMBL SQLite tables into PostgreSQL bronze."""
    resolved_version, sqlite_path = ensure_chembl_sqlite(chembl_version)

    context = ChEMBLLoadContext(
        chembl_version=resolved_version,
        loaded_at=datetime.now(timezone.utc),
        record_limit=validate_record_limit(record_limit),
    )

    postgres_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    postgres_connection = postgres_hook.get_conn()

    try:
        create_standard_bronze_tables(postgres_connection)

        with sqlite3.connect(sqlite_path) as sqlite_connection:
            loader = ChEMBLBronzeLoader(
                postgres_connection=postgres_connection,
                sqlite_connection=sqlite_connection,
                context=context,
            )

            result = loader.load()

        logger.info('Finished full ChEMBL bronze load: %s', result)

        return result

    finally:
        postgres_connection.close()
