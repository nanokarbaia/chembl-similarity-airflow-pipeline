CREATE OR REPLACE VIEW gold.vw_avg_similarity_per_source AS
SELECT
    source_chembl_id,
    AVG(similarity_score) AS avg_similarity_score,
    COUNT(*) AS similar_molecule_count
FROM gold.fact_molecule_similarity
GROUP BY source_chembl_id;


CREATE OR REPLACE VIEW gold.vw_avg_alogp_deviation_per_source AS
SELECT
    f.source_chembl_id,
    AVG(ABS(target_molecule.alogp - source_molecule.alogp))
        AS avg_abs_alogp_deviation,
    COUNT(*) FILTER (
        WHERE source_molecule.alogp IS NOT NULL
          AND target_molecule.alogp IS NOT NULL
    ) AS compared_rows_with_alogp
FROM gold.fact_molecule_similarity f
LEFT JOIN gold.dim_molecule source_molecule
    ON f.source_chembl_id = source_molecule.chembl_id
LEFT JOIN gold.dim_molecule target_molecule
    ON f.target_chembl_id = target_molecule.chembl_id
GROUP BY f.source_chembl_id;


CREATE OR REPLACE VIEW gold.vw_similarity_with_neighbors AS
SELECT
    source_chembl_id,
    target_chembl_id,
    similarity_score,

    LEAD(target_chembl_id) OVER (
        PARTITION BY source_chembl_id
        ORDER BY similarity_rank
    ) AS next_most_similar_target_chembl_id,

    NTH_VALUE(target_chembl_id, 2) OVER (
        PARTITION BY source_chembl_id
        ORDER BY similarity_rank
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS second_most_similar_target_chembl_id

FROM gold.fact_molecule_similarity;


CREATE OR REPLACE VIEW gold.vw_avg_similarity_grouping_sets AS
SELECT
    CASE
        WHEN GROUPING(f.source_chembl_id) = 1 THEN 'TOTAL'
        ELSE f.source_chembl_id
    END AS source_chembl_id,

    CASE
        WHEN GROUPING(source_molecule.aromatic_rings) = 1 THEN 'TOTAL'
        WHEN source_molecule.aromatic_rings IS NULL THEN 'UNKNOWN'
        ELSE source_molecule.aromatic_rings::TEXT
    END AS source_aromatic_rings,

    CASE
        WHEN GROUPING(source_molecule.heavy_atoms) = 1 THEN 'TOTAL'
        WHEN source_molecule.heavy_atoms IS NULL THEN 'UNKNOWN'
        ELSE source_molecule.heavy_atoms::TEXT
    END AS source_heavy_atoms,

    CASE
        WHEN GROUPING(f.source_chembl_id) = 0 THEN 'SOURCE_MOLECULE'
        WHEN GROUPING(source_molecule.aromatic_rings) = 0
         AND GROUPING(source_molecule.heavy_atoms) = 0
            THEN 'SOURCE_AROMATIC_RINGS_HEAVY_ATOMS'
        WHEN GROUPING(source_molecule.heavy_atoms) = 0
            THEN 'SOURCE_HEAVY_ATOMS'
        ELSE 'TOTAL'
    END AS aggregation_level,

    AVG(f.similarity_score) AS avg_similarity_score,
    COUNT(*) AS similarity_rows

FROM gold.fact_molecule_similarity f
LEFT JOIN gold.dim_molecule source_molecule
    ON f.source_chembl_id = source_molecule.chembl_id

GROUP BY GROUPING SETS (
    (f.source_chembl_id),
    (source_molecule.aromatic_rings, source_molecule.heavy_atoms),
    (source_molecule.heavy_atoms),
    ()
);
