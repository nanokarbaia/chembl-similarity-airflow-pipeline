"""Reusable S3 helper functions."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)


def get_s3_client(aws_conn_id: str) -> Any:
    """Create an S3 client from an Airflow AWS connection."""
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    return s3_hook.get_conn()


def list_s3_keys(
    s3_client: Any,
    bucket_name: str,
    prefix: str,
    suffix: str | None = None,
) -> list[str]:
    """List S3 keys under a prefix."""
    paginator = s3_client.get_paginator('list_objects_v2')
    keys: list[str] = []

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']

            if suffix and not key.endswith(suffix):
                continue

            keys.append(key)

    return sorted(keys)


def download_s3_file(
    s3_client: Any,
    bucket_name: str,
    key: str,
    local_path: Path,
) -> Path:
    """Download one S3 object to a local path."""
    local_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(
        'Downloading s3://%s/%s to %s',
        bucket_name,
        key,
        local_path,
    )

    s3_client.download_file(
        bucket_name,
        key,
        str(local_path),
    )

    return local_path


def upload_s3_file(
    s3_client: Any,
    bucket_name: str,
    local_path: Path,
    key: str,
) -> None:
    """Upload one local file to S3."""
    logger.info(
        'Uploading %s to s3://%s/%s',
        local_path,
        bucket_name,
        key,
    )

    s3_client.upload_file(
        str(local_path),
        bucket_name,
        key,
    )


def delete_s3_prefix(
    s3_client: Any,
    bucket_name: str,
    prefix: str,
) -> int:
    """Delete all objects under an S3 prefix."""
    keys = list_s3_keys(
        s3_client=s3_client,
        bucket_name=bucket_name,
        prefix=prefix,
    )

    if not keys:
        logger.info('No objects found under s3://%s/%s', bucket_name, prefix)
        return 0

    logger.info(
        'Deleting %s object(s) from s3://%s/%s',
        len(keys),
        bucket_name,
        prefix,
    )

    for start_index in range(0, len(keys), 1000):
        batch = keys[start_index:start_index + 1000]

        s3_client.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': [{'Key': key} for key in batch],
                'Quiet': True,
            },
        )

    return len(keys)
