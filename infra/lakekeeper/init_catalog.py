"""One-shot, idempotent Lakekeeper setup: bootstrap the server and create the warehouse.

Run through `make catalog-init`. Settings come from the environment (loaded from .env).
Standard library only, so it needs no extra dependency.
"""

import json
import os
import sys
import urllib.error
import urllib.request

BASE = os.environ.get("LAKEKEEPER_URL", "http://localhost:8181")
WAREHOUSE = os.environ.get("LAKEKEEPER_WAREHOUSE", "lakehouse")


def call(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{BASE}{path}", data=data, method=method, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        sys.exit(f"{method} {path} failed: {exc.code} {exc.read().decode()}")
    return json.loads(raw) if raw else {}


def main() -> None:
    if not call("GET", "/management/v1/info")["bootstrapped"]:
        call("POST", "/management/v1/bootstrap", {"accept-terms-of-use": True})
        print("Lakekeeper bootstrapped.")

    profile = {
        "type": "s3",
        "bucket": os.environ["S3_BUCKET"],
        "key-prefix": "warehouse",
        "endpoint": os.environ["S3_ENDPOINT"],
        "region": os.environ.get("S3_REGION", "garage"),
        "path-style-access": True,
        "flavor": "s3-compat",
        # Garage has no STS, so nothing can be vended, and remote signing is off as well: every
        # client signs with its own key, PyIceberg and DuckDB alike (ADR 0003).
        "sts-enabled": False,
        "remote-signing-enabled": False,
    }
    credential = {
        "type": "s3",
        "credential-type": "access-key",
        "aws-access-key-id": os.environ["S3_ACCESS_KEY_ID"],
        "aws-secret-access-key": os.environ["S3_SECRET_ACCESS_KEY"],
    }

    existing = {w["name"]: w for w in call("GET", "/management/v1/warehouse")["warehouses"]}
    if WAREHOUSE not in existing:
        call(
            "POST",
            "/management/v1/warehouse",
            {
                "warehouse-name": WAREHOUSE,
                "storage-profile": profile,
                "storage-credential": credential,
            },
        )
        print(f"Warehouse '{WAREHOUSE}' created.")
        return

    # Bring an older warehouse in line (it was created with remote signing on).
    current = existing[WAREHOUSE]["storage-profile"]
    if current.get("remote-signing-enabled") != profile["remote-signing-enabled"]:
        call(
            "POST",
            f"/management/v1/warehouse/{existing[WAREHOUSE]['id']}/storage",
            {"storage-profile": profile, "storage-credential": credential},
        )
        print(f"Warehouse '{WAREHOUSE}' storage profile updated: remote signing off.")
    else:
        print(f"Warehouse '{WAREHOUSE}' already exists.")


if __name__ == "__main__":
    main()
