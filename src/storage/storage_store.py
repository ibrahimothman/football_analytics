from urllib.parse import urlparse
from io import BytesIO
import pandas as pd
import pyarrow.parquet as pq

from src.storage.s3 import (
    get_s3_client,
)

from src.config.settings import (
    S3_BUCKET,
)


def key_from_s3_uri(
    uri: str,
) -> str:

    parsed = urlparse(uri)

    if parsed.scheme != "s3":
        raise ValueError(
            f"Expected S3 URI, got: {uri}"
        )

    return parsed.path.lstrip("/")

def put_bytes(
    *,
    key: str,
    bytes: bytes,
) -> str:
    """Store bytes in storage and return the key."""

    s3 = get_s3_client()

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=bytes,
    )

    return f"s3://{S3_BUCKET}/{key}"

def get_bytes(
    *,
    uri: str,
) -> bytes:
    """Get bytes from storage and return the bytes."""

    s3 = get_s3_client()

    response = s3.get_object(
        Bucket=S3_BUCKET,
        Key=key_from_s3_uri(uri),
    )

    return response["Body"].read()

def write_parquet(
    *,
    key: str,
    df: pd.DataFrame,
) -> str:
    """Write a pandas DataFrame to storage as a Parquet file."""

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    return put_bytes(
        key=key,
        bytes=buffer.getvalue(),
    )

def read_parquet(
    *,
    uri: str,
) -> pd.DataFrame:
    """Read a pandas DataFrame from storage as a Parquet file."""
    return pd.read_parquet(BytesIO(get_bytes(uri=uri)))

def count_parquet_rows(
    *,
    uri: str,
) -> int:
    """Count the number of rows in a Parquet file."""
    bytes = get_bytes(uri=uri)

    parquet_file = pq.ParquetFile(BytesIO(bytes))

    return parquet_file.metadata.num_rows


