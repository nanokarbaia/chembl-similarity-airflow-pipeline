CREATE TABLE gold.dim_molecule AS
WITH referred_molecules AS (
    SELECT DISTINCT
        UNNEST(ARRAY[source_chembl_id, target_chembl_id]) AS chembl_id
    FROM gold.fact_molecule_similarity
)

SELECT
    rm.chembl_id,
    sm.molecule_type,
    sm.mw_freebase,
    sm.alogp,
    sm.psa,
    sm.cx_logp,
    sm.molecular_species,
    sm.full_mwt,
    sm.aromatic_rings,
    sm.heavy_atoms,
    CURRENT_TIMESTAMP AS loaded_at
FROM referred_molecules rm
LEFT JOIN silver.molecules sm
    ON rm.chembl_id = sm.chembl_id;

ALTER TABLE gold.dim_molecule
    ALTER COLUMN chembl_id SET NOT NULL;

ALTER TABLE gold.dim_molecule
    ADD CONSTRAINT pk_dim_molecule
        PRIMARY KEY (chembl_id);

ALTER TABLE gold.fact_molecule_similarity
    ADD CONSTRAINT fk_fact_source_molecule
        FOREIGN KEY (source_chembl_id)
        REFERENCES gold.dim_molecule (chembl_id);

ALTER TABLE gold.fact_molecule_similarity
    ADD CONSTRAINT fk_fact_target_molecule
        FOREIGN KEY (target_chembl_id)
        REFERENCES gold.dim_molecule (chembl_id);

CREATE INDEX idx_fact_molecule_similarity_target
    ON gold.fact_molecule_similarity (target_chembl_id);

CREATE INDEX idx_fact_molecule_similarity_rank
    ON gold.fact_molecule_similarity (similarity_rank);

ANALYZE gold.fact_molecule_similarity;
ANALYZE gold.dim_molecule;
