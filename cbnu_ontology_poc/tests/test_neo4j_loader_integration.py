from __future__ import annotations

import pytest

from neo4j_loader import (
    clear_database,
    create_schema,
    get_driver,
    load_sample_data,
)


@pytest.fixture(scope="module")
def driver():
    """Provision a real Neo4j driver with freshly loaded sample data."""
    drv = get_driver()
    clear_database(drv)
    create_schema(drv)
    load_sample_data(drv)
    yield drv
    drv.close()


def test_available_recommended_books(driver):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (s:Student)-[:ENROLLED_IN]->(c:Course)-[:HELD_IN_TERM]->(t:Term)
            WITH s, c, t
            MATCH (c)-[:HAS_RECOMMENDED_BOOK]->(b:Book)
            WHERE b.available = true
            RETURN s.name AS studentName,
                   t.name AS termName,
                   c.name AS courseName,
                   b.name AS bookTitle
            LIMIT 5
            """
        ).data()
    assert result, "Expected at least one available recommended book for a student/term."


def test_track_program_scholarship_bundle(driver):
    with driver.session() as session:
        record = session.run(
            """
            MATCH (track:MajorTrack)<-[:SUITABLE_FOR_MAJOR]-(program:NonCurricularProgram)
            OPTIONAL MATCH (program)<-[:REQUIRES_PROGRAM]-(sch:Scholarship)
            RETURN track.name AS trackName,
                   program.name AS programName,
                   program.minYear AS minYear,
                   program.maxYear AS maxYear,
                   collect(DISTINCT sch.name) AS scholarships
            LIMIT 1
            """
        ).single()
    assert record is not None
    assert record["programName"], "Program name should be present."
    assert record["minYear"] <= record["maxYear"]


def test_graduating_student_snapshot(driver):
    with driver.session() as session:
        record = session.run(
            """
            MATCH (s:Student {status: 'graduating'})
            OPTIONAL MATCH (s)-[:ENROLLED_IN]->(c:Course)
            OPTIONAL MATCH (s)-[:PARTICIPATED_IN]->(p:NonCurricularProgram)
            OPTIONAL MATCH (s)-[:RECEIVED_SCHOLARSHIP]->(sch:Scholarship)
            RETURN s.name AS studentName,
                   s.requiredCredits AS requiredCredits,
                   sum(DISTINCT c.credits) AS earnedCredits,
                   count(DISTINCT p) AS programCount,
                   count(DISTINCT sch) AS scholarshipCount
            LIMIT 1
            """
        ).single()
    assert record is not None
    assert record["studentName"]
    assert record["earnedCredits"] is not None


def test_graph_structure_summary(driver):
    with driver.session() as session:
        node_counts = session.run(
            """
            MATCH (n)
            RETURN count(n) AS totalNodes
            """
        ).single()
        rel_counts = session.run(
            """
            MATCH ()-[r]->()
            RETURN count(r) AS totalRelationships
            """
        ).single()
    assert node_counts["totalNodes"] > 0
    assert rel_counts["totalRelationships"] > 0
