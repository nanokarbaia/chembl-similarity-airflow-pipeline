"""Compute Morgan fingerprints for ChEMBL molecules and upload them to S3."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from rdkit import Chem, DataStructs
from rdkit.Chem import rdFingerprintGenerator

from lib.chembl.constants import (
    AWS_CONN_ID,
    DEFAULT_FINGERPRINT_BATCH_SIZE,
    DWH_CONN_ID,
    FINGERPRINT_N_BITS,
    FINGERPRINT_RADIUS,
    FINGERPRINTS_S3_PREFIX,
    S3_BUCKET,
)
from lib.utils.parsing import normalize_chembl_id, parse_positive_int
from lib.utils.s3 import (
    build_s3_folder_prefix,
    delete_s3_prefix,
    get_s3_client,
    upload_s3_file,
)

logger = logging.getLogger(__name__)

SILVER_MOLECULES_QUERY = """
SELECT
    chembl_id,
    canonical_smiles
FROM silver.molecules
WHERE canonical_smiles IS NOT NULL
ORDER BY chembl_id
"""


@dataclass
class FingerprintGenerationStats:
    """Counters collected during fingerprint generation."""

    input_rows: int = 0
    fingerprints: int = 0
    invalid_smiles: int = 0
    files_uploaded: int = 0

    def to_dict(self) -> dict[str, int]:
        """Convert stats to a serializable dictionary."""
        return {
            'input_rows': self.input_rows,
            'fingerprints': self.fingerprints,
            'invalid_smiles': self.invalid_smiles,
            'files_uploaded': self.files_uploaded,
        }


def get_morgan_generator() -> Any:
    """Create Morgan fingerprint generator."""
    return rdFingerprintGenerator.GetMorganGenerator(
        radius=FINGERPRINT_RADIUS,
        fpSize=FINGERPRINT_N_BITS,
    )


def compute_fingerprint_row(
    chembl_id: Any,
    canonical_smiles: Any,
    generator: Any,
) -> dict[str, Any] | None:
    """Compute Morgan fingerprint for one molecule."""
    normalized_chembl_id = normalize_chembl_id(chembl_id)

    if normalized_chembl_id is None:
        return None

    if not canonical_smiles or not str(canonical_smiles).strip():
        return None

    cleaned_smiles = str(canonical_smiles).strip()
    molecule = Chem.MolFromSmiles(cleaned_smiles)

    if molecule is None:
        return None

    fingerprint = generator.GetFingerprint(molecule)

    return {
        'chembl_id': normalized_chembl_id,
        'canonical_smiles': cleaned_smiles,
        'fingerprint_binary': DataStructs.BitVectToBinaryText(fingerprint),
        'fingerprint_on_bits': int(fingerprint.GetNumOnBits()),
        'fingerprint_radius': FINGERPRINT_RADIUS,
        'fingerprint_n_bits': FINGERPRINT_N_BITS,
    }


def build_fingerprint_s3_key(file_count: int) -> str:
    """Build S3 key for one fingerprint parquet file."""
    fingerprint_folder_prefix = build_s3_folder_prefix(FINGERPRINTS_S3_PREFIX)

    return f'{fingerprint_folder_prefix}fingerprints_part_{file_count:05d}.parquet'


def write_fingerprint_batch(
    fingerprint_rows: list[dict[str, Any]],
    output_dir: Path,
    file_count: int,
    s3_client: Any,
) -> None:
    """Write one fingerprint batch to parquet and upload it to S3."""
    local_path = output_dir / f'fingerprints_part_{file_count:05d}.parquet'

    pd.DataFrame(fingerprint_rows).to_parquet(
        local_path,
        engine='pyarrow',
        index=False,
    )

    s3_key = build_fingerprint_s3_key(file_count)

    upload_s3_file(
        s3_client=s3_client,
        bucket_name=S3_BUCKET,
        local_path=local_path,
        key=s3_key,
    )


def process_fingerprint_batch(
    rows: list[tuple[Any, Any]],
    generator: Any,
    stats: FingerprintGenerationStats,
) -> list[dict[str, Any]]:
    """Compute fingerprints for one batch of silver molecules."""
    fingerprint_rows: list[dict[str, Any]] = []

    stats.input_rows += len(rows)

    for chembl_id, canonical_smiles in rows:
        fingerprint_row = compute_fingerprint_row(
            chembl_id=chembl_id,
            canonical_smiles=canonical_smiles,
            generator=generator,
        )

        if fingerprint_row is None:
            stats.invalid_smiles += 1
            continue

        fingerprint_rows.append(fingerprint_row)

    return fingerprint_rows


class FingerprintGeneratorService:
    """Generate Morgan fingerprints from silver molecules and store them in S3."""

    def __init__(
        self,
        batch_size: int,
        output_dir: Path,
    ) -> None:
        self.batch_size = parse_positive_int(batch_size, 'batch_size')
        self.output_dir = output_dir
        self.generator = get_morgan_generator()
        self.stats = FingerprintGenerationStats()
        self.s3_client = get_s3_client(aws_conn_id=AWS_CONN_ID)

    def clear_existing_outputs(self) -> None:
        """Delete old fingerprint files from the S3 output prefix."""
        delete_s3_prefix(
            s3_client=self.s3_client,
            bucket_name=S3_BUCKET,
            prefix=build_s3_folder_prefix(FINGERPRINTS_S3_PREFIX),
        )

    def write_batch(self, fingerprint_rows: list[dict[str, Any]]) -> None:
        """Write a computed fingerprint batch to S3."""
        write_fingerprint_batch(
            fingerprint_rows=fingerprint_rows,
            output_dir=self.output_dir,
            file_count=self.stats.files_uploaded,
            s3_client=self.s3_client,
        )

        self.stats.fingerprints += len(fingerprint_rows)
        self.stats.files_uploaded += 1

    def process_rows(self, rows: list[tuple[Any, Any]]) -> None:
        """Process one database batch and upload it if fingerprints exist."""
        fingerprint_rows = process_fingerprint_batch(
            rows=rows,
            generator=self.generator,
            stats=self.stats,
        )

        if not fingerprint_rows:
            logger.warning(
                'Skipped a batch because no valid fingerprints were generated.'
            )
            return

        self.write_batch(fingerprint_rows)

        logger.info(
            'Processed input rows=%s, valid fingerprints=%s, '
            'invalid_smiles=%s, files_uploaded=%s',
            self.stats.input_rows,
            self.stats.fingerprints,
            self.stats.invalid_smiles,
            self.stats.files_uploaded,
        )


def compute_and_upload_fingerprints(
    batch_size: int = DEFAULT_FINGERPRINT_BATCH_SIZE,
) -> dict[str, int]:
    """Compute Morgan fingerprints for silver molecules and upload to S3."""
    batch_size = parse_positive_int(batch_size, 'batch_size')

    logger.info(
        'Starting fingerprint calculation. radius=%s, n_bits=%s, batch_size=%s',
        FINGERPRINT_RADIUS,
        FINGERPRINT_N_BITS,
        batch_size,
    )

    postgres_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    connection = postgres_hook.get_conn()

    try:
        with TemporaryDirectory(prefix='chembl_fingerprints_') as temp_dir:
            service = FingerprintGeneratorService(
                batch_size=batch_size,
                output_dir=Path(temp_dir),
            )

            service.clear_existing_outputs()

            with connection.cursor(
                name='silver_molecule_fingerprint_cursor'
            ) as cursor:
                cursor.itersize = batch_size
                cursor.execute(SILVER_MOLECULES_QUERY)

                while True:
                    rows = cursor.fetchmany(batch_size)

                    if not rows:
                        break

                    service.process_rows(rows)

            if service.stats.fingerprints == 0:
                raise ValueError('No fingerprints were generated.')

            result = service.stats.to_dict()

    finally:
        connection.close()

    logger.info('Finished fingerprint calculation: %s', result)

    return result
