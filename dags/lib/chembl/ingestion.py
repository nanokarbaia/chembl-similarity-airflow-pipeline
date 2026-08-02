"""Airflow callables for ChEMBL ingestion."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from lib.chembl.constants import DEFAULT_CHEMBL_PAGE_LIMIT
from lib.utils.parsing import normalize_text, parse_positive_int

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChEMBLIngestionConfig:
    """Validated runtime configuration for ChEMBL bronze ingestion."""

    chembl_version: str | None
    record_limit: int | None
    page_limit: int

    @classmethod
    def from_params(
        cls,
        chembl_version: Any = None,
        record_limit: Any = None,
        page_limit: Any = DEFAULT_CHEMBL_PAGE_LIMIT,
    ) -> ChEMBLIngestionConfig:
        """Build ingestion config from Airflow parameters."""
        normalized_chembl_version = normalize_text(chembl_version)

        return cls(
            chembl_version=normalized_chembl_version,
            record_limit=parse_optional_positive_int(
                value=record_limit,
                parameter_name='record_limit',
            ),
            page_limit=parse_positive_int(
                value=page_limit,
                parameter_name='page_limit',
            ),
        )


def parse_optional_positive_int(value: Any, parameter_name: str) -> int | None:
    """Parse an optional positive integer parameter."""
    normalized_value = normalize_text(value)

    if normalized_value is None:
        return None

    return parse_positive_int(
        value=normalized_value,
        parameter_name=parameter_name,
    )


def ingest_chembl_bronze(
    chembl_version: str | None = None,
    record_limit: int | None = None,
    page_limit: int = DEFAULT_CHEMBL_PAGE_LIMIT,
) -> dict[str, int]:
    """Load required ChEMBL tables into the bronze DWH layer."""
    config = ChEMBLIngestionConfig.from_params(
        chembl_version=chembl_version,
        record_limit=record_limit,
        page_limit=page_limit,
    )

    logger.info(
        'Starting ChEMBL bronze ingestion. '
        'chembl_version=%s, record_limit=%s, page_limit=%s',
        config.chembl_version,
        config.record_limit,
        config.page_limit,
    )

    if config.record_limit is not None:
        from lib.chembl.api_loader import load_chembl_api_sample_to_bronze

        result = load_chembl_api_sample_to_bronze(
            record_limit=config.record_limit,
            page_limit=config.page_limit,
        )
    else:
        from lib.chembl.bronze_loader import load_required_chembl_tables_to_bronze

        result = load_required_chembl_tables_to_bronze(
            chembl_version=config.chembl_version,
            record_limit=None,
        )

    logger.info('Finished ChEMBL bronze ingestion: %s', result)

    return result
