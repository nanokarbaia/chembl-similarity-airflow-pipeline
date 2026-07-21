"""Airflow callables for ChEMBL ingestion."""

from __future__ import annotations

import logging

from lib.chembl.constants import DEFAULT_CHEMBL_PAGE_LIMIT

logger = logging.getLogger(__name__)


def ingest_chembl_bronze(**context) -> dict[str, int]:
    """Load required ChEMBL tables into the bronze DWH layer."""
    params = context['params']

    chembl_version = params.get('chembl_version')
    record_limit = params.get('ingest_record_limit')
    page_limit = int(params.get('chembl_page_limit') or DEFAULT_CHEMBL_PAGE_LIMIT)

    logger.info(
        'Starting ChEMBL bronze ingestion. Version=%s, record_limit=%s, page_limit=%s',
        chembl_version,
        record_limit,
        page_limit,
    )

    if record_limit is not None:
        from lib.chembl.api_loader import load_chembl_api_sample_to_bronze

        result = load_chembl_api_sample_to_bronze(
            record_limit=int(record_limit),
            page_limit=page_limit,
        )
    else:
        from lib.chembl.bronze_loader import load_required_chembl_tables_to_bronze

        result = load_required_chembl_tables_to_bronze(
            chembl_version=chembl_version,
            record_limit=None,
        )

    logger.info('Finished ChEMBL bronze ingestion: %s', result)

    return result
