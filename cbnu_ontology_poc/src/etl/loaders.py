from __future__ import annotations

from pathlib import Path
from typing import Final

import pandas as pd

from src import config

_DATASET_FILENAMES: Final[dict[str, str]] = {
    "students": "students.csv",
    "courses": "courses.csv",
    "books": "books.csv",
    "programs": "programs.csv",
    "scholarships": "scholarships.csv",
    "departments": "departments.csv",
}

def _resolve_csv_path(filename: str) -> Path:
    return config.DATA_DIR / filename


def load_csv(filename: str) -> pd.DataFrame:
    csv_path = _resolve_csv_path(filename)
    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    return pd.read_csv(csv_path)


def load_students() -> pd.DataFrame:
    return load_csv(_DATASET_FILENAMES["students"])


def load_courses() -> pd.DataFrame:
    return load_csv(_DATASET_FILENAMES["courses"])


def load_books() -> pd.DataFrame:
    return load_csv(_DATASET_FILENAMES["books"])


def load_programs() -> pd.DataFrame:
    return load_csv(_DATASET_FILENAMES["programs"])


def load_scholarships() -> pd.DataFrame:
    return load_csv(_DATASET_FILENAMES["scholarships"])


def load_departments() -> pd.DataFrame:
    return load_csv(_DATASET_FILENAMES["departments"])

__all__ = [
    "load_csv",
    "load_students",
    "load_courses",
    "load_books",
    "load_programs",
    "load_scholarships",
    "load_departments",
]
