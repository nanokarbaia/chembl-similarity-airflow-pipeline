CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.molecules CASCADE;

CREATE TABLE silver.molecules (
    chembl_id TEXT NOT NULL,
    molecule_type TEXT,
    canonical_smiles TEXT NOT NULL,
    standard_inchi TEXT,
    standard_inchi_key TEXT,
    mw_freebase NUMERIC,
    alogp NUMERIC,
    psa NUMERIC,
    cx_logp NUMERIC,
    molecular_species TEXT,
    full_mwt NUMERIC,
    aromatic_rings INTEGER,
    heavy_atoms INTEGER,
    prepared_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_silver_molecules
        PRIMARY KEY (chembl_id)
);

WITH molecule_dictionary AS (
    SELECT DISTINCT ON (UPPER(TRIM(chembl_id)))
        UPPER(TRIM(chembl_id)) AS chembl_id,
        NULLIF(TRIM(molecule_type), '') AS molecule_type
    FROM bronze.molecule_dictionary
    WHERE NULLIF(TRIM(chembl_id), '') IS NOT NULL
      AND UPPER(TRIM(chembl_id)) ~ '^CHEMBL[0-9]+$'
    ORDER BY UPPER(TRIM(chembl_id)), loaded_at DESC NULLS LAST
),

compound_structures AS (
    SELECT DISTINCT ON (UPPER(TRIM(chembl_id)))
        UPPER(TRIM(chembl_id)) AS chembl_id,
        NULLIF(TRIM(canonical_smiles), '') AS canonical_smiles,
        NULLIF(TRIM(standard_inchi), '') AS standard_inchi,
        NULLIF(TRIM(standard_inchi_key), '') AS standard_inchi_key
    FROM bronze.compound_structures
    WHERE NULLIF(TRIM(chembl_id), '') IS NOT NULL
      AND UPPER(TRIM(chembl_id)) ~ '^CHEMBL[0-9]+$'
      AND NULLIF(TRIM(canonical_smiles), '') IS NOT NULL
    ORDER BY UPPER(TRIM(chembl_id)), loaded_at DESC NULLS LAST
),

compound_properties AS (
    SELECT DISTINCT ON (UPPER(TRIM(chembl_id)))
        UPPER(TRIM(chembl_id)) AS chembl_id,

        CASE
            WHEN NULLIF(TRIM(mw_freebase), '') ~
                 '^([0-9]+(\.[0-9]+)?|\.[0-9]+)([eE][+-]?[0-9]+)?$'
                THEN TRIM(mw_freebase)::NUMERIC
        END AS mw_freebase,

        CASE
            WHEN NULLIF(TRIM(alogp), '') ~
                 '^[+-]?([0-9]+(\.[0-9]+)?|\.[0-9]+)([eE][+-]?[0-9]+)?$'
                THEN TRIM(alogp)::NUMERIC
        END AS alogp,

        CASE
            WHEN NULLIF(TRIM(psa), '') ~
                 '^([0-9]+(\.[0-9]+)?|\.[0-9]+)([eE][+-]?[0-9]+)?$'
                THEN TRIM(psa)::NUMERIC
        END AS psa,

        CASE
            WHEN NULLIF(TRIM(cx_logp), '') ~
                 '^[+-]?([0-9]+(\.[0-9]+)?|\.[0-9]+)([eE][+-]?[0-9]+)?$'
                THEN TRIM(cx_logp)::NUMERIC
        END AS cx_logp,

        NULLIF(TRIM(molecular_species), '') AS molecular_species,

        CASE
            WHEN NULLIF(TRIM(full_mwt), '') ~
                 '^([0-9]+(\.[0-9]+)?|\.[0-9]+)([eE][+-]?[0-9]+)?$'
                THEN TRIM(full_mwt)::NUMERIC
        END AS full_mwt,

        CASE
            WHEN NULLIF(TRIM(aromatic_rings), '') ~ '^[0-9]+$'
                THEN TRIM(aromatic_rings)::INTEGER
        END AS aromatic_rings,

        CASE
            WHEN NULLIF(TRIM(heavy_atoms), '') ~ '^[0-9]+$'
                THEN TRIM(heavy_atoms)::INTEGER
        END AS heavy_atoms

    FROM bronze.compound_properties
    WHERE NULLIF(TRIM(chembl_id), '') IS NOT NULL
      AND UPPER(TRIM(chembl_id)) ~ '^CHEMBL[0-9]+$'
    ORDER BY UPPER(TRIM(chembl_id)), loaded_at DESC NULLS LAST
)

INSERT INTO silver.molecules (
    chembl_id,
    molecule_type,
    canonical_smiles,
    standard_inchi,
    standard_inchi_key,
    mw_freebase,
    alogp,
    psa,
    cx_logp,
    molecular_species,
    full_mwt,
    aromatic_rings,
    heavy_atoms
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
    cp.heavy_atoms
FROM compound_structures cs
LEFT JOIN molecule_dictionary md
    ON cs.chembl_id = md.chembl_id
LEFT JOIN compound_properties cp
    ON cs.chembl_id = cp.chembl_id;

CREATE INDEX idx_silver_molecules_standard_inchi_key
    ON silver.molecules (standard_inchi_key)
    WHERE standard_inchi_key IS NOT NULL;

CREATE INDEX idx_silver_molecules_molecule_type
    ON silver.molecules (molecule_type)
    WHERE molecule_type IS NOT NULL;

ANALYZE silver.molecules;
