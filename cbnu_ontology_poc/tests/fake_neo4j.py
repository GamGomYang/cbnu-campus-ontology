from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List

from src.ontology_schema import NODE_KEY_MAP, NodeLabel, RelType


@dataclass
class FakeNeo4jResult:
    records: List[Dict[str, Any]]

    def single(self) -> Dict[str, Any] | None:
        return self.records[0] if self.records else None

    def data(self) -> List[Dict[str, Any]]:
        return list(self.records)


class FakeNeo4jClient:
    def __init__(self) -> None:
        self.nodes: Dict[str, Dict[str, Dict[str, Any]]] = {
            label.value: {} for label in NodeLabel
        }
        self.relationships: List[tuple[str, str, str, str, str]] = []

    def close(self) -> None:  # pragma: no cover - nothing to close
        return None

    def run(self, query: str, parameters: Dict[str, Any] | None = None) -> FakeNeo4jResult:
        parameters = parameters or {}
        normalized_query = " ".join(query.split())

        if "collect(DISTINCT c) AS courses" in query and "RETURN s," in query:
            return self._handle_student_context(parameters)
        if "RETURN c," in query and "collect(DISTINCT b) AS books" in query:
            return self._handle_course_resources(parameters)
        if "DETACH DELETE" in normalized_query:
            for label_nodes in self.nodes.values():
                label_nodes.clear()
            self.relationships.clear()
            return FakeNeo4jResult([])

        for label in NodeLabel:
            if f"MERGE (n:{label.value}" in query and "SET n +=" in query:
                return self._handle_node_merge(label, parameters)

        if "MERGE (from)-[" in query and "MATCH (from:" in query:
            return self._handle_relationship_merge(query, parameters)

        if "RETURN count" in normalized_query:
            relationship_match = re.search(
                r"MATCH\s*\(\w+:(?P<from_label>\w+)\s*\{\s*(?P<from_key>\w+):\s*'(?P<from_id>[^']+)'\s*\}\)\s*-\s*\[:(?P<rel_type>\w+)\]\s*->\s*\(\w+:(?P<to_label>\w+)\s*\{\s*(?P<to_key>\w+):\s*'(?P<to_id>[^']+)'\s*\}\)",
                normalized_query,
            )
            if relationship_match:
                info = relationship_match.groupdict()
                exists = any(
                    rel == (
                        info["from_label"],
                        info["from_id"],
                        info["rel_type"],
                        info["to_label"],
                        info["to_id"],
                    )
                    for rel in self.relationships
                )
                return FakeNeo4jResult([{"count": 1 if exists else 0}])

            label_match = re.search(r"MATCH\s*\(\w+:(?P<label>\w+)\)", normalized_query)
            alias_match = re.search(
                r"RETURN\s*count\(\w+\)\s*AS\s*(?P<alias>\w+)",
                normalized_query,
                re.IGNORECASE,
            )
            if label_match and alias_match:
                label = label_match.group("label")
                alias = alias_match.group("alias")
                count = len(self.nodes[label])
                return FakeNeo4jResult([{alias: count}])

        return FakeNeo4jResult([])

    # --- Helpers -----------------------------------------------------------------

    def _handle_node_merge(self, label: NodeLabel, parameters: Dict[str, Any]) -> FakeNeo4jResult:
        rows = parameters.get("rows", [])
        key_field = NODE_KEY_MAP[label]
        storage = self.nodes[label.value]
        for row in rows:
            key_value = str(row[key_field])
            storage[key_value] = dict(row)
        return FakeNeo4jResult([])

    def _handle_relationship_merge(self, query: str, parameters: Dict[str, Any]) -> FakeNeo4jResult:
        from_label = _extract_label(query, "from")
        to_label = _extract_label(query, "to")
        rel_type = _extract_relationship_type(query)
        from_id = str(parameters.get("from_id"))
        to_id = str(parameters.get("to_id"))
        self.relationships.append((from_label, from_id, rel_type, to_label, to_id))
        return FakeNeo4jResult([])

    def _handle_student_context(self, parameters: Dict[str, Any]) -> FakeNeo4jResult:
        student_id = str(parameters.get("student_id"))
        student = self._get_node(NodeLabel.STUDENT, student_id)
        if not student:
            return FakeNeo4jResult([])

        courses = self._collect_outgoing(NodeLabel.STUDENT, student_id, RelType.ENROLLED_IN, NodeLabel.COURSE)

        books: list[dict[str, Any]] = []
        programs: list[dict[str, Any]] = []
        scholarships: list[dict[str, Any]] = []

        for course in courses:
            course_id = str(course[NODE_KEY_MAP[NodeLabel.COURSE]])
            books.extend(self._collect_outgoing(NodeLabel.COURSE, course_id, RelType.USES_BOOK, NodeLabel.BOOK))
            programs.extend(self._collect_outgoing(NodeLabel.COURSE, course_id, RelType.RELATED_PROGRAM, NodeLabel.PROGRAM))
            scholarships.extend(
                self._collect_incoming(
                    NodeLabel.COURSE,
                    course_id,
                    RelType.REQUIRES_COURSE,
                    NodeLabel.SCHOLARSHIP,
                )
            )

        return FakeNeo4jResult(
            [
                {
                    "s": student,
                    "courses": courses,
                    "books": books,
                    "programs": programs,
                    "scholarships": scholarships,
                }
            ]
        )

    def _handle_course_resources(self, parameters: Dict[str, Any]) -> FakeNeo4jResult:
        course_id = str(parameters.get("course_id"))
        course = self._get_node(NodeLabel.COURSE, course_id)
        if not course:
            return FakeNeo4jResult([])

        books = self._collect_outgoing(NodeLabel.COURSE, course_id, RelType.USES_BOOK, NodeLabel.BOOK)
        programs = self._collect_outgoing(NodeLabel.COURSE, course_id, RelType.RELATED_PROGRAM, NodeLabel.PROGRAM)
        scholarships = self._collect_incoming(
            NodeLabel.COURSE,
            course_id,
            RelType.REQUIRES_COURSE,
            NodeLabel.SCHOLARSHIP,
        )

        return FakeNeo4jResult(
            [
                {
                    "c": course,
                    "books": books,
                    "programs": programs,
                    "scholarships": scholarships,
                }
            ]
        )

    def _get_node(self, label: NodeLabel, node_id: str | int) -> dict[str, Any] | None:
        storage = self.nodes[label.value]
        return storage.get(str(node_id))

    def _collect_outgoing(
        self,
        from_label: NodeLabel,
        from_id: str | int,
        rel_type: RelType,
        to_label: NodeLabel,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        seen: set[str] = set()
        target_key = NODE_KEY_MAP[to_label]
        for stored_from, stored_from_id, stored_rel, stored_to_label, stored_to_id in self.relationships:
            if (
                stored_from == from_label.value
                and stored_from_id == str(from_id)
                and stored_rel == rel_type.value
                and stored_to_label == to_label.value
            ):
                node = self._get_node(to_label, stored_to_id)
                if node:
                    key = str(node[target_key])
                    if key not in seen:
                        seen.add(key)
                        results.append(node)
        return results

    def _collect_incoming(
        self,
        to_label: NodeLabel,
        to_id: str | int,
        rel_type: RelType,
        from_label: NodeLabel,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        seen: set[str] = set()
        source_key = NODE_KEY_MAP[from_label]
        for stored_from, stored_from_id, stored_rel, stored_to_label, stored_to_id in self.relationships:
            if (
                stored_from == from_label.value
                and stored_to_label == to_label.value
                and stored_to_id == str(to_id)
                and stored_rel == rel_type.value
            ):
                node = self._get_node(from_label, stored_from_id)
                if node:
                    key = str(node[source_key])
                    if key not in seen:
                        seen.add(key)
                        results.append(node)
        return results


def _extract_label(query: str, alias: str) -> str:
    pattern = re.compile(rf"{alias}:(\w+)")
    match = pattern.search(query)
    if not match:  # pragma: no cover - guard clause
        raise ValueError(f"Unable to parse label for alias '{alias}' in query: {query}")
    return match.group(1)


def _extract_relationship_type(query: str) -> str:
    pattern = re.compile(r"-\s*\[:(\w+)\]")
    match = pattern.search(query)
    if not match:  # pragma: no cover - guard clause
        raise ValueError(f"Unable to parse relationship type from query: {query}")
    return match.group(1)


__all__ = ["FakeNeo4jClient", "FakeNeo4jResult"]
