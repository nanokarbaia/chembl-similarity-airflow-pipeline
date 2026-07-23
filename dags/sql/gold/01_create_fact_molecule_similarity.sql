CREATE TABLE gold.fact_molecule_similarity (
    source_chembl_id VARCHAR(64) NOT NULL,
    target_chembl_id VARCHAR(64) NOT NULL,
    similarity_score DOUBLE PRECISION NOT NULL,
    similarity_rank INTEGER NOT NULL,
    has_duplicates_of_last_largest_score BOOLEAN NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_fact_molecule_similarity
        PRIMARY KEY (source_chembl_id, target_chembl_id),

    CONSTRAINT uq_fact_source_rank
        UNIQUE (source_chembl_id, similarity_rank),

    CONSTRAINT chk_similarity_score_range
        CHECK (similarity_score >= 0 AND similarity_score <= 1),

    CONSTRAINT chk_similarity_rank_positive
        CHECK (similarity_rank > 0)
);
