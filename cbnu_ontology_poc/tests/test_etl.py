from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src import config
from src.etl import loaders, validators

@pytest.fixture()
def sample_data_dir(tmp_path, monkeypatch):
    datasets: dict[str, list[dict[str, object]]] = {
        "students.csv": [
            {"student_id": 1, "name": "Alice", "dept_id": "CS", "year": 3, "status": "active"},
            {"student_id": 2, "name": "Bob", "dept_id": "EE", "year": 2, "status": "active"},
        ],
        "courses.csv": [
            {"course_id": "CS101", "name": "Intro CS", "dept_id": "CS", "credit": 3, "type": "core"},
            {"course_id": "EE200", "name": "Circuits", "dept_id": "EE", "credit": 3, "type": "core"},
        ],
        "books.csv": [
            {"book_id": "B1", "title": "Data Science", "author": "Kwak", "topic": "AI", "available": True},
        ],
        "programs.csv": [
            {"program_id": "P1", "name": "AI Track", "target_dept_id": "CS", "skill_tag": "AI"},
        ],
        "scholarships.csv": [
            {"scholarship_id": "S1", "name": "Merit", "min_gpa": 3.5, "required_credit": 30},
        ],
        "departments.csv": [
            {"dept_id": "CS", "name": "Computer Science"},
            {"dept_id": "EE", "name": "Electrical Engineering"},
        ],
    }

    for filename, rows in datasets.items():
        df = pd.DataFrame(rows)
        df.to_csv(tmp_path / filename, index=False)

    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    yield tmp_path

def test_load_students_basic(sample_data_dir):
    df = loaders.load_students()
    assert not df.empty
    assert validators.REQUIRED_COLUMNS["students"].issubset(set(df.columns))

def test_validate_required_columns(sample_data_dir):
    df = loaders.load_courses()
    required = {"course_id", "name", "dept_id"}
    assert validators.validate_required_columns(df, required)

def test_validate_no_null_in_key(sample_data_dir):
    df = loaders.load_courses()
    assert validators.validate_no_null_in_key(df, "course_id")

def test_load_csv_missing_file(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    with pytest.raises(FileNotFoundError):
        loaders.load_csv("not_exist.csv")
