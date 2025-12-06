from __future__ import annotations

from typing import Callable, Dict

import pandas as pd

from src import config
from src.etl import loaders
from src.ontology_schema import NODE_KEY_MAP, NODE_SCHEMAS, NodeLabel, RelType

from .neo4j_client import Neo4jClient

DatasetLoader = Callable[[], pd.DataFrame]


_DATASET_LOADERS: Dict[NodeLabel, DatasetLoader] = {
    NodeLabel.STUDENT: loaders.load_students,
    NodeLabel.COURSE: loaders.load_courses,
    NodeLabel.BOOK: loaders.load_books,
    NodeLabel.PROGRAM: loaders.load_programs,
    NodeLabel.SCHOLARSHIP: loaders.load_scholarships,
    NodeLabel.DEPARTMENT: loaders.load_departments,
}


def clear_database(client: Neo4jClient) -> None:
    client.run("MATCH (n) DETACH DELETE n")


def load_nodes(client: Neo4jClient) -> None:
    for label, loader in _DATASET_LOADERS.items():
        schema = NODE_SCHEMAS[label]
        df = loader()
        if df.empty:
            continue

        records = df.loc[:, schema.properties].to_dict("records")
        cypher = (
            f"UNWIND $rows AS row\n"
            f"MERGE (n:{label.value} {{{schema.key}: row.{schema.key}}})\n"
            f"SET n += row"
        )
        client.run(cypher, {"rows": records})


def load_relationships(client: Neo4jClient) -> None:
    relations_path = config.DATA_DIR / "relations.csv"
    if not relations_path.is_file():
        raise FileNotFoundError(f"relations.csv not found in data directory: {relations_path}")

    df = pd.read_csv(relations_path)
    if df.empty:
        return

    for row in df.to_dict("records"):
        try:
            from_label = NodeLabel(row["from_label"])
            to_label = NodeLabel(row["to_label"])
            rel_type = RelType(row["rel_type"])
        except ValueError as exc:  # pragma: no cover - invalid schema definitions
            raise ValueError(f"Invalid label or relationship type in row: {row}") from exc

        from_key = NODE_KEY_MAP[from_label]
        to_key = NODE_KEY_MAP[to_label]
        parameters = {"from_id": row["from_id"], "to_id": row["to_id"]}
        cypher = (
            f"MATCH (from:{from_label.value} {{{from_key}: $from_id}})\n"
            f"MATCH (to:{to_label.value} {{{to_key}: $to_id}})\n"
            f"MERGE (from)-[:{rel_type.value}]->(to)"
        )
        client.run(cypher, parameters)


__all__ = ["clear_database", "load_nodes", "load_relationships"]
