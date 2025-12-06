from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Final


class NodeLabel(str, Enum):
    STUDENT = "Student"
    COURSE = "Course"
    BOOK = "Book"
    PROGRAM = "Program"
    SCHOLARSHIP = "Scholarship"
    DEPARTMENT = "Department"


class RelType(str, Enum):
    ENROLLED_IN = "ENROLLED_IN"
    BELONGS_TO = "BELONGS_TO"
    USES_BOOK = "USES_BOOK"
    RELATED_PROGRAM = "RELATED_PROGRAM"
    REQUIRES_COURSE = "REQUIRES_COURSE"
    PARTICIPATED_IN = "PARTICIPATED_IN"


@dataclass(frozen=True)
class NodeSchema:
    label: NodeLabel
    key: str
    properties: tuple[str, ...]


NODE_SCHEMAS: Final[dict[NodeLabel, NodeSchema]] = {
    NodeLabel.STUDENT: NodeSchema(
        label=NodeLabel.STUDENT,
        key="student_id",
        properties=("student_id", "name", "dept_id", "year", "status"),
    ),
    NodeLabel.COURSE: NodeSchema(
        label=NodeLabel.COURSE,
        key="course_id",
        properties=("course_id", "name", "dept_id", "credit", "type"),
    ),
    NodeLabel.BOOK: NodeSchema(
        label=NodeLabel.BOOK,
        key="book_id",
        properties=("book_id", "title", "author", "topic", "available"),
    ),
    NodeLabel.PROGRAM: NodeSchema(
        label=NodeLabel.PROGRAM,
        key="program_id",
        properties=("program_id", "name", "target_dept_id", "skill_tag"),
    ),
    NodeLabel.SCHOLARSHIP: NodeSchema(
        label=NodeLabel.SCHOLARSHIP,
        key="scholarship_id",
        properties=("scholarship_id", "name", "min_gpa", "required_credit"),
    ),
    NodeLabel.DEPARTMENT: NodeSchema(
        label=NodeLabel.DEPARTMENT,
        key="dept_id",
        properties=("dept_id", "name"),
    ),
}

NODE_KEY_MAP: Final[dict[NodeLabel, str]] = {
    label: schema.key for label, schema in NODE_SCHEMAS.items()
}

__all__ = [
    "NodeLabel",
    "RelType",
    "NodeSchema",
    "NODE_SCHEMAS",
    "NODE_KEY_MAP",
]
