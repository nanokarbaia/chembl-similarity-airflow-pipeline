"""Prepare cleaned molecule data in the silver DWH layer."""

from __future__ import annotations

import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook

from lib.chembl.constants import DWH_CONN_ID

logger = logging.getLogger(__name__)


PREPARE_SILVER_MOLECULES_SQL = """
DROP TABLE IF EXISTS silver.molecules CASCADE;

CREATE TABLE silver.molecules AS
WITH molecule_dictionary AS (
    SELECT DISTINCT ON (TRIM(chembl_id))
        TRIM(chembl_id) AS chembl_id,
        NULLIF(TRIM(molecule_type), '') AS molecule_type
    FROM bronze.molecule_dictionary
    WHERE NULLIF(TRIM(chembl_id), '') IS NOT NULL
    ORDER BY TRIM(chembl_id), loaded_at DESC
),

compound_structures AS (
    SELECT DISTINCT ON (TRIM(chembl_id))
        TRIM(chembl_id) AS chembl_id,
        NULLIF(TRIM(canonical_smiles), '') AS canonical_smiles,
        NULLIF(TRIM(standard_inchi), '') AS standard_inchi,
        NULLIF(TRIM(standard_inchi_key), '') AS standard_inchi_key
    FROM bronze.compound_structures
    WHERE NULLIF(TRIM(chembl_id), '') IS NOT NULL
      AND NULLIF(TRIM(canonical_smiles), '') IS NOT NULL
    ORDER BY TRIM(chembl_id), loaded_at DESC
),

compound_properties AS (
    SELECT DISTINCT ON (TRIM(chembl_id))
        TRIM(chembl_id) AS chembl_id,

        CASE
            WHEN NULLIF(TRIM(mw_freebase), '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                THEN TRIM(mw_freebase)::NUMERIC
        END AS mw_freebase,

        CASE
            WHEN NULLIF(TRIM(alogp), '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                THEN TRIM(alogp)::NUMERIC
        END AS alogp,

        CASE
            WHEN NULLIF(TRIM(psa), '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                THEN TRIM(psa)::NUMERIC
        END AS psa,

        CASE
            WHEN NULLIF(TRIM(cx_logp), '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                THEN TRIM(cx_logp)::NUMERIC
        END AS cx_logp,

        NULLIF(TRIM(molecular_species), '') AS molecular_species,

        CASE
            WHEN NULLIF(TRIM(full_mwt), '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                THEN TRIM(full_mwt)::NUMERIC
        END AS full_mwt,

        CASE
            WHEN NULLIF(TRIM(aromatic_rings), '') ~ '^-?[0-9]+$'
                THEN TRIM(aromatic_rings)::INTEGER
        END AS aromatic_rings,

        CASE
            WHEN NULLIF(TRIM(heavy_atoms), '') ~ '^-?[0-9]+$'
                THEN TRIM(heavy_atoms)::INTEGER
        END AS heavy_atoms

    FROM bronze.compound_properties
    WHERE NULLIF(TRIM(chembl_id), '') IS NOT NULL
    ORDER BY TRIM(chembl_id), loaded_at DESC
)

SELECT
    cs.chembl_id,
    md.molecule_type,
    cs.canonical_smiles,
    cs.standard_inchi,
    cs.standard_inchi_key,
    cp.mw_freebase,
    cp.alogp,
    cp.psa,
    cp.cx_logp,
    cp.molecular_species,
    cp.full_mwt,
    cp.aromatic_rings,
    cp.heavy_atoms,
    CURRENT_TIMESTAMP AS prepared_at
FROM compound_structures cs
LEFT JOIN molecule_dictionary md
    ON cs.chembl_id = md.chembl_id
LEFT JOIN compound_properties cp
    ON cs.chembl_id = cp.chembl_id;

CREATE INDEX idx_silver_molecules_chembl_id
    ON silver.molecules (chembl_id);

CREATE INDEX idx_silver_molecules_standard_inchi_key
    ON silver.molecules (standard_inchi_key);

ANALYZE silver.molecules;
"""


def prepare_silver_molecules() -> int:
    """Create silver.molecules from bronze ChEMBL tables."""
    logger.info('Preparing silver.molecules table.')

    postgres_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)

    postgres_hook.run(PREPARE_SILVER_MOLECULES_SQL)

    row_count = postgres_hook.get_first(
        'SELECT COUNT(*) FROM silver.molecules'
    )[0]

    logger.info('silver.molecules prepared. Row count: %s', row_count)

    if row_count == 0:
        raise ValueError('silver.molecules is empty after preparation.')

    return row_count
