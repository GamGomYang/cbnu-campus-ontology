from __future__ import annotations

from typing import Final

import pandas as pd

REQUIRED_COLUMNS: Final[dict[str, set[str]]] = {
    "students": {"student_id", "name", "dept_id", "year", "status"},
    "courses": {"course_id", "name", "dept_id", "credit", "type"},
    "books": {"book_id", "title", "author", "topic", "available"},
    "programs": {"program_id", "name", "target_dept_id", "skill_tag"},
    "scholarships": {"scholarship_id", "name", "min_gpa", "required_credit"},
    "departments": {"dept_id", "name"},
}

def validate_required_columns(df: pd.DataFrame, required: set[str]) -> bool:
    available = {str(column) for column in df.columns}
    missing = required.difference(available)
    return not missing

def validate_no_null_in_key(df: pd.DataFrame, key_col: str) -> bool:
    if key_col not in df.columns:
        raise KeyError(f"Column '{key_col}' is not present in the DataFrame")
    return not df[key_col].isna().any()

__all__ = [
    "REQUIRED_COLUMNS",
    "validate_required_columns",
    "validate_no_null_in_key",
]
