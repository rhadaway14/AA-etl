from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterator


def iter_csv_rows(csv_path: str | Path) -> Iterator[tuple[int, dict[str, str]]]:
    path = Path(csv_path)
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"CSV has no header row: {path}")
        for idx, row in enumerate(reader, start=1):
            yield idx, row
