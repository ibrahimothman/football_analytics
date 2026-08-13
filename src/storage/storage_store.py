from urllib.parse import urlparse

from src.storage.s3 import (
    get_s3_client,
)

from src.config.settings import (
    S3_BUCKET,
)


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


def key_from_s3_uri(
    uri: str,
) -> str:

    parsed = urlparse(uri)

    if parsed.scheme != "s3":
        raise ValueError(
            f"Expected S3 URI, got: {uri}"
        )

    return parsed.path.lstrip("/")


