from __future__ import annotations

import pandas as pd
import pytest

from src import config
from src.ontology_schema import NodeLabel, RelType


@pytest.fixture()
def sample_graph_data(tmp_path, monkeypatch):
    datasets = {
        "students.csv": [
            {"student_id": "20240001", "name": "Alice", "dept_id": "CSE", "year": 3, "status": "active"},
            {"student_id": "20240002", "name": "Bob", "dept_id": "EEE", "year": 2, "status": "active"},
        ],
        "courses.csv": [
            {"course_id": "CSE101", "name": "Intro CSE", "dept_id": "CSE", "credit": 3, "type": "core"},
            {"course_id": "EEE200", "name": "Circuits", "dept_id": "EEE", "credit": 3, "type": "core"},
        ],
        "books.csv": [
            {"book_id": "B001", "title": "Graph DBs", "author": "Kim", "topic": "DB", "available": True},
        ],
        "programs.csv": [
            {"program_id": "PRG1", "name": "AI Track", "target_dept_id": "CSE", "skill_tag": "AI"},
        ],
        "scholarships.csv": [
            {"scholarship_id": "SCH1", "name": "Merit", "min_gpa": 3.5, "required_credit": 30},
        ],
        "departments.csv": [
            {"dept_id": "CSE", "name": "Computer Science"},
            {"dept_id": "EEE", "name": "Electrical Engineering"},
        ],
    }

    for filename, rows in datasets.items():
        pd.DataFrame(rows).to_csv(tmp_path / filename, index=False)

    relations = [
        {
            "from_label": NodeLabel.STUDENT.value,
            "from_id": "20240001",
            "rel_type": RelType.ENROLLED_IN.value,
            "to_label": NodeLabel.COURSE.value,
            "to_id": "CSE101",
        },
        {
            "from_label": NodeLabel.COURSE.value,
            "from_id": "CSE101",
            "rel_type": RelType.USES_BOOK.value,
            "to_label": NodeLabel.BOOK.value,
            "to_id": "B001",
        },
        {
            "from_label": NodeLabel.COURSE.value,
            "from_id": "CSE101",
            "rel_type": RelType.RELATED_PROGRAM.value,
            "to_label": NodeLabel.PROGRAM.value,
            "to_id": "PRG1",
        },
        {
            "from_label": NodeLabel.SCHOLARSHIP.value,
            "from_id": "SCH1",
            "rel_type": RelType.REQUIRES_COURSE.value,
            "to_label": NodeLabel.COURSE.value,
            "to_id": "CSE101",
        },
    ]
    pd.DataFrame(relations).to_csv(tmp_path / "relations.csv", index=False)

    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    counts = {
        NodeLabel.STUDENT: len(datasets["students.csv"]),
        NodeLabel.COURSE: len(datasets["courses.csv"]),
        NodeLabel.BOOK: len(datasets["books.csv"]),
        NodeLabel.PROGRAM: len(datasets["programs.csv"]),
        NodeLabel.SCHOLARSHIP: len(datasets["scholarships.csv"]),
        NodeLabel.DEPARTMENT: len(datasets["departments.csv"]),
    }

    return {
        "counts": counts,
        "relations": {
            "enrollment": relations[0],
            "course_book": relations[1],
            "course_program": relations[2],
            "scholarship_course": relations[3],
        },
        "data_dir": tmp_path,
    }
