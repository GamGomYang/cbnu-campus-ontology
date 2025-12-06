from __future__ import annotations

from typing import Any, Iterable, Mapping

from src.graph.neo4j_client import Neo4jClient


def _serialize_node(node: Any) -> dict[str, Any] | None:
    if node is None:
        return None
    if isinstance(node, dict):
        return dict(node)
    if hasattr(node, "items"):
        return {k: v for k, v in node.items()}  # type: ignore[union-attr]
    try:  # pragma: no cover - best-effort fallback for other node objects
        return dict(node)
    except Exception:
        return None


def _serialize_collection(nodes: Iterable[Any] | None) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    if not nodes:
        return result
    for node in nodes:
        serialized = _serialize_node(node)
        if serialized is not None:
            result.append(serialized)
    return result


def get_student_context(client: Neo4jClient, student_id: str) -> dict[str, Any]:
    query = """
    MATCH (s:Student {student_id: $student_id})
    OPTIONAL MATCH (s)-[:ENROLLED_IN]->(c:Course)
    OPTIONAL MATCH (c)-[:USES_BOOK]->(b:Book)
    OPTIONAL MATCH (c)-[:RELATED_PROGRAM]->(p:Program)
    OPTIONAL MATCH (sc:Scholarship)-[:REQUIRES_COURSE]->(c)
    RETURN s,
           collect(DISTINCT c) AS courses,
           collect(DISTINCT b) AS books,
           collect(DISTINCT p) AS programs,
           collect(DISTINCT sc) AS scholarships
    """
    result = client.run(query, {"student_id": student_id})
    record: Mapping[str, Any] | None = result.single()
    if not record:
        return {}

    student = _serialize_node(record.get("s"))
    if student is None:
        return {}

    return {
        "student": student,
        "courses": _serialize_collection(record.get("courses")),
        "books": _serialize_collection(record.get("books")),
        "programs": _serialize_collection(record.get("programs")),
        "scholarships": _serialize_collection(record.get("scholarships")),
    }


def get_course_resources(client: Neo4jClient, course_id: str) -> dict[str, Any]:
    query = """
    MATCH (c:Course {course_id: $course_id})
    OPTIONAL MATCH (c)-[:USES_BOOK]->(b:Book)
    OPTIONAL MATCH (c)-[:RELATED_PROGRAM]->(p:Program)
    OPTIONAL MATCH (sc:Scholarship)-[:REQUIRES_COURSE]->(c)
    RETURN c,
           collect(DISTINCT b) AS books,
           collect(DISTINCT p) AS programs,
           collect(DISTINCT sc) AS scholarships
    """
    result = client.run(query, {"course_id": course_id})
    record: Mapping[str, Any] | None = result.single()
    if not record:
        return {}

    course = _serialize_node(record.get("c"))
    if course is None:
        return {}

    return {
        "course": course,
        "books": _serialize_collection(record.get("books")),
        "programs": _serialize_collection(record.get("programs")),
        "scholarships": _serialize_collection(record.get("scholarships")),
    }


__all__ = ["get_student_context", "get_course_resources"]
