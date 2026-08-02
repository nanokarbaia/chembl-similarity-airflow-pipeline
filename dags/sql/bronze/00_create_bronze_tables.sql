CREATE SCHEMA IF NOT EXISTS bronze;

DROP TABLE IF EXISTS bronze.chembl_id_lookup CASCADE;
DROP TABLE IF EXISTS bronze.molecule_dictionary CASCADE;
DROP TABLE IF EXISTS bronze.compound_properties CASCADE;
DROP TABLE IF EXISTS bronze.compound_structures CASCADE;

CREATE TABLE bronze.chembl_id_lookup (
    chembl_id TEXT,
    entity_type TEXT,
    status TEXT,
    resource_url TEXT,
    raw_record JSONB,
    source_system TEXT NOT NULL,
    source_chembl_version TEXT,
    loaded_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE bronze.molecule_dictionary (
    molregno TEXT,
    chembl_id TEXT,
    molecule_type TEXT,
    pref_name TEXT,
    max_phase TEXT,
    therapeutic_flag TEXT,
    raw_record JSONB,
    source_system TEXT NOT NULL,
    source_chembl_version TEXT,
    loaded_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE bronze.compound_properties (
    molregno TEXT,
    chembl_id TEXT,
    mw_freebase TEXT,
    alogp TEXT,
    psa TEXT,
    cx_logp TEXT,
    molecular_species TEXT,
    full_mwt TEXT,
    aromatic_rings TEXT,
    heavy_atoms TEXT,
    raw_record JSONB,
    source_system TEXT NOT NULL,
    source_chembl_version TEXT,
    loaded_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE bronze.compound_structures (
    molregno TEXT,
    chembl_id TEXT,
    canonical_smiles TEXT,
    standard_inchi TEXT,
    standard_inchi_key TEXT,
    raw_record JSONB,
    source_system TEXT NOT NULL,
    source_chembl_version TEXT,
    loaded_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_bronze_chembl_id_lookup_chembl_id
    ON bronze.chembl_id_lookup (chembl_id);

CREATE INDEX idx_bronze_molecule_dictionary_chembl_id
    ON bronze.molecule_dictionary (chembl_id);

CREATE INDEX idx_bronze_molecule_dictionary_molregno
    ON bronze.molecule_dictionary (molregno);

CREATE INDEX idx_bronze_compound_properties_chembl_id
    ON bronze.compound_properties (chembl_id);

CREATE INDEX idx_bronze_compound_properties_molregno
    ON bronze.compound_properties (molregno);

CREATE INDEX idx_bronze_compound_structures_chembl_id
    ON bronze.compound_structures (chembl_id);

CREATE INDEX idx_bronze_compound_structures_molregno
    ON bronze.compound_structures (molregno);

ANALYZE bronze.chembl_id_lookup;
ANALYZE bronze.molecule_dictionary;
ANALYZE bronze.compound_properties;
ANALYZE bronze.compound_structures;
