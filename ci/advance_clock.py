"""Move the simulated clock forward to the end of an arrival day; it never moves back.

The clock file holds one ISO timestamp, the end of the latest arrival day loaded. The dbt
anomaly and freshness tests read it (SIMULATED_NOW) as "now", because time in the data is simulated.
Standard library only, so make can call it with any python3.

    python3 ci/advance_clock.py 2026-02-01 data/simulated_now   # -> 2026-02-02T00:00:00+00:00
"""

import sys
from datetime import date, timedelta
from pathlib import Path


def main(day: str, clock_file: str) -> None:
    path = Path(clock_file)
    new = f"{date.fromisoformat(day) + timedelta(days=1)}T00:00:00+00:00"
    old = path.read_text().strip() if path.exists() else ""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(max(old, new) + "\n")  # ISO timestamps in one offset sort as text


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
