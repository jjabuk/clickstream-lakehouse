"""The landing zone: raw daily JSONL files in object storage, exactly as the source delivered them.

Layout: s3://<bucket>/<prefix>/dt=<arrival day>/events.jsonl. The file is the unit of replay:
loading a day twice reads the same file, and regenerating a day overwrites it with the same bytes.
The S3 endpoint is the one URL every client uses (ADR 0012).
"""

from collections.abc import Mapping
from datetime import date
from urllib.parse import urlparse

import pyarrow.fs as pafs

DEFAULT_PREFIX = "raw/clickstream"


def filesystem(env: Mapping[str, str]) -> pafs.S3FileSystem:
    endpoint = urlparse(env["S3_ENDPOINT"])
    return pafs.S3FileSystem(
        access_key=env["S3_ACCESS_KEY_ID"],
        secret_key=env["S3_SECRET_ACCESS_KEY"],
        endpoint_override=endpoint.netloc,
        scheme=endpoint.scheme,
        region=env.get("S3_REGION", "garage"),
    )


def landing_path(env: Mapping[str, str], day: date, prefix: str = DEFAULT_PREFIX) -> str:
    return f"{env['S3_BUCKET']}/{prefix}/dt={day.isoformat()}/events.jsonl"


def write(fs: pafs.S3FileSystem, path: str, payload: bytes) -> None:
    with fs.open_output_stream(path) as out:
        out.write(payload)
