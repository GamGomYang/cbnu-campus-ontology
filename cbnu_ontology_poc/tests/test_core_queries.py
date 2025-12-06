from __future__ import annotations

from src.graph import graph_builder
from src.queries import core_queries

from .fake_neo4j import FakeNeo4jClient


def _prepare_graph(sample_graph_data):
    client = FakeNeo4jClient()
    graph_builder.clear_database(client)
    graph_builder.load_nodes(client)
    graph_builder.load_relationships(client)
    return client


def test_get_student_context_basic(sample_graph_data):
    client = _prepare_graph(sample_graph_data)
    context = core_queries.get_student_context(client, "20240001")
    assert isinstance(context, dict)
    assert context.get("student") is not None
    assert str(context["student"]["student_id"]) == "20240001"
    assert len(context.get("courses", [])) >= 1
    assert len(context.get("books", [])) >= 1


def test_get_student_context_not_found(sample_graph_data):
    client = _prepare_graph(sample_graph_data)
    context = core_queries.get_student_context(client, "99999999")
    assert context == {}


def test_get_course_resources_basic(sample_graph_data):
    client = _prepare_graph(sample_graph_data)
    resources = core_queries.get_course_resources(client, "CSE101")
    assert isinstance(resources, dict)
    assert resources.get("course") is not None
    assert str(resources["course"]["course_id"]) == "CSE101"
    assert len(resources.get("books", [])) >= 1
    assert len(resources.get("scholarships", [])) >= 1
