CREATE SCHEMA IF NOT EXISTS gold;

DROP VIEW IF EXISTS gold.vw_similarity_pivot_10_sources CASCADE;
DROP VIEW IF EXISTS gold.vw_avg_similarity_per_source CASCADE;
DROP VIEW IF EXISTS gold.vw_avg_alogp_deviation_per_source CASCADE;
DROP VIEW IF EXISTS gold.vw_similarity_with_neighbors CASCADE;
DROP VIEW IF EXISTS gold.vw_avg_similarity_grouping_sets CASCADE;

DROP TABLE IF EXISTS gold.fact_molecule_similarity CASCADE;
DROP TABLE IF EXISTS gold.dim_molecule CASCADE;
