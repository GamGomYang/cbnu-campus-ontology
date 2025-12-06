from __future__ import annotations

from src.ontology_schema import NODE_KEY_MAP, NodeLabel

from .neo4j_client import Neo4jClient


def create_constraints(client: Neo4jClient) -> None:
    for label, key in NODE_KEY_MAP.items():
        cypher = (
            f"CREATE CONSTRAINT IF NOT EXISTS "
            f"FOR (n:{label.value}) REQUIRE n.{key} IS UNIQUE"
        )
        client.run(cypher)


__all__ = ["create_constraints"]
