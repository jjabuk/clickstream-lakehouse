"""Fail a pull request that breaks a model contract without a new version (ADR 0025).

dbt Core stops a breaking change to a contracted model when it compares state; dbt v2 does not
(a changed column type is not even `state:modified`). So CI compares the manifest of the pull
request with the production manifest of main:

- a column removed from a contracted model, or its data type changed, is breaking;
- so is a contract that is no longer enforced;
- a contracted model (or version) that disappears is breaking, unless another version of the
  same model still has its columns with the same types (moving a model to versions keeps the
  old contract as v1), or it was a version past its deprecation date.

Adding columns, models or versions is never breaking.

Usage: python ci/check_contracts.py BASE_MANIFEST NEW_MANIFEST
"""

import json
import sys
from datetime import date
from pathlib import Path

Key = tuple[str, str | None]  # (model name, version)


def contracts(manifest: dict) -> dict[Key, dict]:
    """The models with an enforced contract: their columns' types and deprecation date."""
    found = {}
    for node in manifest["nodes"].values():
        if node["resource_type"] != "model":
            continue
        if not node["config"].get("contract", {}).get("enforced"):
            continue
        version = node.get("version")
        found[(node["name"], None if version is None else str(version))] = {
            "columns": {
                name: (col.get("data_type") or "").strip().lower()
                for name, col in node["columns"].items()
            },
            "deprecation_date": node.get("deprecation_date"),
        }
    return found


def _label(key: Key) -> str:
    name, version = key
    return name if version is None else f"{name} v{version}"


def _column_breaks(old: dict[str, str], new: dict[str, str]) -> list[str]:
    breaks = []
    for column, data_type in old.items():
        if column not in new:
            breaks.append(f"column {column} removed")
        elif data_type and new[column] != data_type:
            breaks.append(f"column {column} changed type {data_type} -> {new[column]}")
    return breaks


def breaking_changes(base: dict, new: dict, today: date) -> list[str]:
    old_contracts, new_contracts = contracts(base), contracts(new)
    if not old_contracts:
        raise ValueError("the base manifest has no contracted models; is it the right file?")
    all_new_models = {
        (n["name"], None if n.get("version") is None else str(n["version"]))
        for n in new["nodes"].values()
        if n["resource_type"] == "model"
    }

    problems = []
    for key, old in old_contracts.items():
        if key in new_contracts:
            breaks = _column_breaks(old["columns"], new_contracts[key]["columns"])
            problems += [f"{_label(key)}: {b}" for b in breaks]
            continue
        if key in all_new_models:
            problems.append(f"{_label(key)}: the contract is no longer enforced")
            continue
        deprecated = old["deprecation_date"]
        if deprecated and date.fromisoformat(str(deprecated)[:10]) <= today:
            continue  # a version removed after its deprecation date
        keeps_it = [
            k
            for k, c in new_contracts.items()
            if k[0] == key[0] and not _column_breaks(old["columns"], c["columns"])
        ]
        if not keeps_it:
            problems.append(f"{_label(key)}: removed, and no version of {key[0]} keeps its columns")
    return problems


def main() -> None:
    base_path, new_path = (Path(p) for p in sys.argv[1:3])
    base, new = (json.loads(p.read_text()) for p in (base_path, new_path))
    problems = breaking_changes(base, new, date.today())
    if problems:
        print("Breaking changes to model contracts (add a new model version instead):")
        for problem in problems:
            print(f"  - {problem}")
        sys.exit(1)
    print(f"contracts: no breaking change against {base_path}")


if __name__ == "__main__":
    main()
