"""Load a development sample of ChEMBL data from the ChEMBL REST API."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Iterable

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json, execute_values

from lib.chembl.bronze_schema import create_standard_bronze_tables
from lib.chembl.constants import (
    BRONZE_SCHEMA,
    CHEMBL_API_BASE_URL,
    DEFAULT_CHEMBL_PAGE_LIMIT,
    DWH_CONN_ID,
)

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
DEFAULT_MAX_RETRIES = 5
DEFAULT_REQUEST_TIMEOUT = 120


def fetch_chembl_page(
    session: requests.Session,
    resource: str,
    limit: int,
    offset: int,
    max_retries: int = DEFAULT_MAX_RETRIES,
    request_timeout: int = DEFAULT_REQUEST_TIMEOUT,
) -> dict[str, Any]:
    """Fetch one page from the ChEMBL API with retry and backoff."""
    url = f'{CHEMBL_API_BASE_URL}/{resource}.json'
    params = {
        'limit': limit,
        'offset': offset,
    }

    last_exception: Exception | None = None
    last_response: requests.Response | None = None

    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(
                url,
                params=params,
                timeout=request_timeout,
            )
            last_response = response

        except (
            requests.ConnectionError,
            requests.Timeout,
        ) as exception:
            last_exception = exception
            wait_seconds = min(2 ** attempt, 60)

            logger.warning(
                'ChEMBL API request failed. '
                'resource=%s, limit=%s, offset=%s, attempt=%s/%s. '
                'Retrying in %s seconds. Error: %s',
                resource,
                limit,
                offset,
                attempt,
                max_retries,
                wait_seconds,
                exception,
            )

            time.sleep(wait_seconds)
            continue

        if response.status_code not in RETRYABLE_STATUS_CODES:
            response.raise_for_status()
            return response.json()

        wait_seconds = min(2 ** attempt, 60)

        logger.warning(
            'ChEMBL API returned retryable status %s. '
            'resource=%s, limit=%s, offset=%s, attempt=%s/%s. '
            'Retrying in %s seconds.',
            response.status_code,
            resource,
            limit,
            offset,
            attempt,
            max_retries,
            wait_seconds,
        )

        time.sleep(wait_seconds)

    if last_response is not None:
        last_response.raise_for_status()

    if last_exception is not None:
        raise last_exception

    raise RuntimeError(
        f'Failed to fetch ChEMBL records for resource={resource}, '
        f'limit={limit}, offset={offset}'
    )


def fetch_chembl_records(
    resource: str,
    records_key: str,
    record_limit: int,
    page_limit: int = DEFAULT_CHEMBL_PAGE_LIMIT,
) -> Iterable[dict[str, Any]]:
    """Fetch records from the ChEMBL API using pagination."""
    if record_limit < 1:
        raise ValueError('record_limit must be at least 1.')

    if page_limit < 1:
        raise ValueError('page_limit must be at least 1.')

    total_records = 0
    offset = 0

    with requests.Session() as session:
        while total_records < record_limit:
            current_limit = min(page_limit, record_limit - total_records)

            payload = fetch_chembl_page(
                session=session,
                resource=resource,
                limit=current_limit,
                offset=offset,
            )

            records = payload.get(records_key, [])

            if not records:
                logger.warning(
                    'No records returned from ChEMBL API. '
                    'resource=%s, records_key=%s, offset=%s, limit=%s',
                    resource,
                    records_key,
                    offset,
                    current_limit,
                )
                break

            for record in records:
                yield record
                total_records += 1

                if total_records >= record_limit:
                    break

            if not payload.get('page_meta', {}).get('next'):
                break

            offset += len(records)

    logger.info(
        'Fetched %s record(s) from ChEMBL resource=%s.',
        total_records,
        resource,
    )


def insert_rows(
    connection,
    table_name: str,
    columns: list[str],
    rows: list[tuple],
) -> int:
    """Insert rows into PostgreSQL using execute_values."""
    if not rows:
        return 0

    column_list = ', '.join(columns)

    query = f'''
        INSERT INTO {BRONZE_SCHEMA}.{table_name} ({column_list})
        VALUES %s
    '''

    with connection.cursor() as cursor:
        execute_values(cursor, query, rows, page_size=1000)

    connection.commit()

    return len(rows)


def load_chembl_id_lookup_sample(
    connection,
    record_limit: int,
    page_limit: int,
    loaded_at: datetime,
) -> int:
    """Load chembl_id_lookup sample from ChEMBL API."""
    rows = []

    for record in fetch_chembl_records(
        resource='chembl_id_lookup',
        records_key='chembl_id_lookups',
        record_limit=record_limit,
        page_limit=page_limit,
    ):
        rows.append(
            (
                record.get('chembl_id'),
                record.get('entity_type'),
                record.get('status'),
                record.get('resource_url'),
                Json(record),
                'chembl_api',
                None,
                loaded_at,
            )
        )

    return insert_rows(
        connection=connection,
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
        rows=rows,
    )


def load_molecule_sample(
    connection,
    record_limit: int,
    page_limit: int,
    loaded_at: datetime,
) -> dict[str, int]:
    """Load molecule dictionary, properties and structures from ChEMBL API."""
    molecule_rows = []
    property_rows = []
    structure_rows = []

    for record in fetch_chembl_records(
        resource='molecule',
        records_key='molecules',
        record_limit=record_limit,
        page_limit=page_limit,
    ):
        chembl_id = record.get('molecule_chembl_id')

        molecule_rows.append(
            (
                None,
                chembl_id,
                record.get('molecule_type'),
                record.get('pref_name'),
                record.get('max_phase'),
                record.get('therapeutic_flag'),
                Json(record),
                'chembl_api',
                None,
                loaded_at,
            )
        )

        properties = record.get('molecule_properties') or {}
        property_rows.append(
            (
                None,
                chembl_id,
                properties.get('mw_freebase'),
                properties.get('alogp'),
                properties.get('psa'),
                properties.get('cx_logp'),
                properties.get('molecular_species'),
                properties.get('full_mwt'),
                properties.get('aromatic_rings'),
                properties.get('heavy_atoms'),
                Json(properties),
                'chembl_api',
                None,
                loaded_at,
            )
        )

        structures = record.get('molecule_structures') or {}
        structure_rows.append(
            (
                None,
                chembl_id,
                structures.get('canonical_smiles'),
                structures.get('standard_inchi'),
                structures.get('standard_inchi_key'),
                Json(structures),
                'chembl_api',
                None,
                loaded_at,
            )
        )

    molecule_count = insert_rows(
        connection=connection,
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
        rows=molecule_rows,
    )

    property_count = insert_rows(
        connection=connection,
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
        rows=property_rows,
    )

    structure_count = insert_rows(
        connection=connection,
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
        rows=structure_rows,
    )

    return {
        'molecule_dictionary': molecule_count,
        'compound_properties': property_count,
        'compound_structures': structure_count,
    }


def load_chembl_api_sample_to_bronze(
    record_limit: int,
    page_limit: int = DEFAULT_CHEMBL_PAGE_LIMIT,
) -> dict[str, int]:
    """Load a limited ChEMBL API sample into bronze tables."""
    logger.info(
        'Loading ChEMBL API sample. record_limit=%s, page_limit=%s',
        record_limit,
        page_limit,
    )

    postgres_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    connection = postgres_hook.get_conn()
    loaded_at = datetime.now(timezone.utc)

    try:
        create_standard_bronze_tables(connection)

        chembl_id_lookup_count = load_chembl_id_lookup_sample(
            connection=connection,
            record_limit=record_limit,
            page_limit=page_limit,
            loaded_at=loaded_at,
        )

        molecule_counts = load_molecule_sample(
            connection=connection,
            record_limit=record_limit,
            page_limit=page_limit,
            loaded_at=loaded_at,
        )

        result = {
            'chembl_id_lookup': chembl_id_lookup_count,
            **molecule_counts,
        }

        logger.info('Finished ChEMBL API sample load: %s', result)

        return result

    finally:
        connection.close()
