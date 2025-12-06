from __future__ import annotations

from src.graph import graph_builder
from src.ontology_schema import NODE_KEY_MAP, NodeLabel

from .fake_neo4j import FakeNeo4jClient


def test_load_nodes_creates_correct_counts(sample_graph_data):
    client = FakeNeo4jClient()
    graph_builder.clear_database(client)
    graph_builder.load_nodes(client)

    for label, expected_count in sample_graph_data["counts"].items():
        result = client.run(f"MATCH (n:{label.value}) RETURN count(n) AS count")
        assert result.single()["count"] == expected_count


def test_load_relationships_creates_edges(sample_graph_data):
    client = FakeNeo4jClient()
    graph_builder.clear_database(client)
    graph_builder.load_nodes(client)
    graph_builder.load_relationships(client)

    relation = sample_graph_data["relations"]["enrollment"]
    from_label = relation["from_label"]
    to_label = relation["to_label"]
    from_key = NODE_KEY_MAP[NodeLabel(from_label)]
    to_key = NODE_KEY_MAP[NodeLabel(to_label)]
    query = (
        f"MATCH (s:{from_label} {{{from_key}: '{relation['from_id']}'}})"
        f"-[:{relation['rel_type']}]->"
        f"(c:{to_label} {{{to_key}: '{relation['to_id']}'}}) "
        "RETURN count(*) AS count"
    )
    result = client.run(query)
    assert result.single()["count"] == 1
