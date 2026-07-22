"""Compute Morgan fingerprints for ChEMBL molecules and upload them to S3."""

from __future__ import annotations

import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
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

logger = logging.getLogger(__name__)


def get_morgan_generator() -> Any:
    """Create Morgan fingerprint generator."""
    return rdFingerprintGenerator.GetMorganGenerator(
        radius=FINGERPRINT_RADIUS,
        fpSize=FINGERPRINT_N_BITS,
    )


def compute_fingerprint_row(
    chembl_id: str,
    canonical_smiles: str,
    generator: Any,
) -> dict[str, Any] | None:
    """Compute Morgan fingerprint for one molecule."""
    molecule = Chem.MolFromSmiles(canonical_smiles)

    if molecule is None:
        return None

    fingerprint = generator.GetFingerprint(molecule)

    return {
        'chembl_id': chembl_id,
        'canonical_smiles': canonical_smiles,
        'fingerprint_binary': DataStructs.BitVectToBinaryText(fingerprint),
        'fingerprint_on_bits': int(fingerprint.GetNumOnBits()),
        'fingerprint_radius': FINGERPRINT_RADIUS,
        'fingerprint_n_bits': FINGERPRINT_N_BITS,
    }


def delete_existing_fingerprint_files(s3_hook: S3Hook) -> None:
    """Delete old fingerprint files from S3 prefix before uploading new files."""
    existing_keys = s3_hook.list_keys(
        bucket_name=S3_BUCKET,
        prefix=f'{FINGERPRINTS_S3_PREFIX}/',
    )

    if not existing_keys:
        logger.info('No existing fingerprint files found in S3.')
        return

    logger.info(
        'Deleting %s existing fingerprint file(s) from s3://%s/%s/',
        len(existing_keys),
        S3_BUCKET,
        FINGERPRINTS_S3_PREFIX,
    )

    s3_hook.delete_objects(
        bucket=S3_BUCKET,
        keys=existing_keys,
    )


def upload_file_to_s3(
    s3_hook: S3Hook,
    local_path: Path,
    s3_key: str,
) -> None:
    """Upload a local file to S3."""
    logger.info(
        'Uploading fingerprint file to s3://%s/%s',
        S3_BUCKET,
        s3_key,
    )

    s3_hook.load_file(
        filename=str(local_path),
        key=s3_key,
        bucket_name=S3_BUCKET,
        replace=True,
    )


def write_fingerprint_batch(
    fingerprint_rows: list[dict[str, Any]],
    output_dir: Path,
    file_count: int,
    s3_hook: S3Hook,
) -> None:
    """Write one fingerprint batch to parquet and upload it to S3."""
    local_path = output_dir / f'fingerprints_part_{file_count:05d}.parquet'

    pd.DataFrame(fingerprint_rows).to_parquet(
        local_path,
        engine='pyarrow',
        index=False,
    )

    s3_key = (
        f'{FINGERPRINTS_S3_PREFIX}/'
        f'fingerprints_part_{file_count:05d}.parquet'
    )

    upload_file_to_s3(
        s3_hook=s3_hook,
        local_path=local_path,
        s3_key=s3_key,
    )


def compute_and_upload_fingerprints(
    batch_size: int = DEFAULT_FINGERPRINT_BATCH_SIZE,
) -> dict[str, int]:
    """Compute Morgan fingerprints for silver molecules and upload to S3."""
    batch_size = int(batch_size)

    logger.info(
        'Starting fingerprint calculation. radius=%s, n_bits=%s, batch_size=%s',
        FINGERPRINT_RADIUS,
        FINGERPRINT_N_BITS,
        batch_size,
    )

    postgres_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    generator = get_morgan_generator()

    total_input_rows = 0
    total_fingerprints = 0
    invalid_smiles = 0
    file_count = 0

    delete_existing_fingerprint_files(s3_hook)

    connection = postgres_hook.get_conn()

    try:
        with TemporaryDirectory(prefix='chembl_fingerprints_') as temp_dir:
            output_dir = Path(temp_dir)

            with connection.cursor(name='silver_molecule_fingerprint_cursor') as cursor:
                cursor.itersize = batch_size
                cursor.execute(
                    """
                    SELECT
                        chembl_id,
                        canonical_smiles
                    FROM silver.molecules
                    WHERE canonical_smiles IS NOT NULL
                    ORDER BY chembl_id
                    """
                )

                while True:
                    rows = cursor.fetchmany(batch_size)

                    if not rows:
                        break

                    total_input_rows += len(rows)
                    fingerprint_rows = []

                    for chembl_id, canonical_smiles in rows:
                        fingerprint_row = compute_fingerprint_row(
                            chembl_id=chembl_id,
                            canonical_smiles=canonical_smiles,
                            generator=generator,
                        )

                        if fingerprint_row is None:
                            invalid_smiles += 1
                            continue

                        fingerprint_rows.append(fingerprint_row)

                    if not fingerprint_rows:
                        continue

                    write_fingerprint_batch(
                        fingerprint_rows=fingerprint_rows,
                        output_dir=output_dir,
                        file_count=file_count,
                        s3_hook=s3_hook,
                    )

                    total_fingerprints += len(fingerprint_rows)
                    file_count += 1

                    logger.info(
                        'Processed input rows=%s, valid fingerprints=%s, '
                        'invalid_smiles=%s',
                        total_input_rows,
                        total_fingerprints,
                        invalid_smiles,
                    )

    finally:
        connection.close()

    if total_fingerprints == 0:
        raise ValueError('No fingerprints were generated.')

    result = {
        'input_rows': total_input_rows,
        'fingerprints': total_fingerprints,
        'invalid_smiles': invalid_smiles,
        'files_uploaded': file_count,
    }

    logger.info('Finished fingerprint calculation: %s', result)

    return result
