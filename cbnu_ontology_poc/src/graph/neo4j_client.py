from __future__ import annotations

from typing import Any, Mapping, Optional

try:
    from neo4j import GraphDatabase
    from neo4j.exceptions import Neo4jError
except ImportError:  # pragma: no cover - handled at runtime
    GraphDatabase = None  # type: ignore[assignment]
    Neo4jError = Exception  # type: ignore[misc, assignment]

from src import config


class Neo4jClient:
    def __init__(
        self,
        uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        if GraphDatabase is None:  # pragma: no cover - requires driver install
            raise RuntimeError(
                "neo4j driver is not installed. Please install 'neo4j' package to use Neo4jClient."
            )

        self._uri = uri or config.NEO4J_URI
        self._user = user or config.NEO4J_USER
        self._password = password or config.NEO4J_PASSWORD
        self._driver = GraphDatabase.driver(
            self._uri,
            auth=(self._user, self._password),
        )

    def close(self) -> None:
        if self._driver is not None:
            self._driver.close()

    def run(self, query: str, parameters: Optional[Mapping[str, Any]] = None):
        params = dict(parameters or {})
        with self._driver.session() as session:
            return session.run(query, params)


__all__ = ["Neo4jClient", "Neo4jError"]
