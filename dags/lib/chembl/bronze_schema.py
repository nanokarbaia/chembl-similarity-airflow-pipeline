"""Bronze table definitions for ChEMBL ingestion."""

from __future__ import annotations

from lib.chembl.constants import BRONZE_SCHEMA


def create_standard_bronze_tables(connection) -> None:
    """Create standardized bronze tables used by both API and full dump ingestion."""
    with connection.cursor() as cursor:
        cursor.execute(
            f'''
            DROP TABLE IF EXISTS {BRONZE_SCHEMA}.chembl_id_lookup CASCADE;
            DROP TABLE IF EXISTS {BRONZE_SCHEMA}.molecule_dictionary CASCADE;
            DROP TABLE IF EXISTS {BRONZE_SCHEMA}.compound_properties CASCADE;
            DROP TABLE IF EXISTS {BRONZE_SCHEMA}.compound_structures CASCADE;

            CREATE TABLE {BRONZE_SCHEMA}.chembl_id_lookup (
                chembl_id TEXT,
                entity_type TEXT,
                status TEXT,
                resource_url TEXT,
                raw_record JSONB,
                source_system TEXT NOT NULL,
                source_chembl_version TEXT,
                loaded_at TIMESTAMP NOT NULL
            );

            CREATE TABLE {BRONZE_SCHEMA}.molecule_dictionary (
                molregno TEXT,
                chembl_id TEXT,
                molecule_type TEXT,
                pref_name TEXT,
                max_phase TEXT,
                therapeutic_flag TEXT,
                raw_record JSONB,
                source_system TEXT NOT NULL,
                source_chembl_version TEXT,
                loaded_at TIMESTAMP NOT NULL
            );

            CREATE TABLE {BRONZE_SCHEMA}.compound_properties (
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
                loaded_at TIMESTAMP NOT NULL
            );

            CREATE TABLE {BRONZE_SCHEMA}.compound_structures (
                molregno TEXT,
                chembl_id TEXT,
                canonical_smiles TEXT,
                standard_inchi TEXT,
                standard_inchi_key TEXT,
                raw_record JSONB,
                source_system TEXT NOT NULL,
                source_chembl_version TEXT,
                loaded_at TIMESTAMP NOT NULL
            );

            CREATE INDEX idx_bronze_molecule_dictionary_chembl_id
                ON {BRONZE_SCHEMA}.molecule_dictionary (chembl_id);

            CREATE INDEX idx_bronze_compound_properties_chembl_id
                ON {BRONZE_SCHEMA}.compound_properties (chembl_id);

            CREATE INDEX idx_bronze_compound_structures_chembl_id
                ON {BRONZE_SCHEMA}.compound_structures (chembl_id);
            '''
        )

    connection.commit()
    