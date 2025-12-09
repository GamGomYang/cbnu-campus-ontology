"""
Populate a large-scale CBNU campus ontology knowledge graph and run validation queries.

Requirements:
    pip install neo4j
"""

from __future__ import annotations

import os
import random
from itertools import cycle
from typing import Any, Dict, Iterable, List

from neo4j import Driver, GraphDatabase

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4j")


def chunked(seq: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    """Yield successive chunks from a list."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def run_batch(session, query: str, rows: List[Dict[str, Any]], batch_size: int = 500) -> None:
    """Execute a parameterized query in batches."""
    if not rows:
        return
    for batch in chunked(rows, batch_size):
        session.run(query, rows=batch)


def get_driver() -> Driver:
    """Initialize the Neo4j driver."""
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()
    return driver


def clear_database(driver: Driver) -> None:
    """Remove every node and relationship from the database."""
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    print("Cleared existing database state.")


def create_schema(driver: Driver) -> None:
    """Create uniqueness constraints for core labels."""
    constraints = {
        "student_id_cons": "Student",
        "professor_id_cons": "Professor",
        "course_id_cons": "Course",
        "book_id_cons": "Book",
        "program_id_cons": "NonCurricularProgram",
        "scholarship_id_cons": "Scholarship",
        "event_id_cons": "AcademicEvent",
        "department_id_cons": "Department",
        "college_id_cons": "College",
        "majortrack_id_cons": "MajorTrack",
        "term_id_cons": "Term",
    }
    with driver.session() as session:
        for name, label in constraints.items():
            session.run(
                f"CREATE CONSTRAINT {name} IF NOT EXISTS "
                f"FOR (n:{label}) REQUIRE n.id IS UNIQUE"
            )
    print("Ensured schema constraints are in place.")


def load_sample_data(driver: Driver) -> None:
    """Generate a large synthetic campus ontology and load it into Neo4j."""
    random.seed(2024)

    colleges = [
        {"id": "COL-ENG", "name": "College of Engineering", "type": "Engineering", "dean": "Prof. Seo"},
        {"id": "COL-ICT", "name": "College of ICT Convergence", "type": "ICT", "dean": "Prof. Lee"},
        {"id": "COL-HUM", "name": "College of Humanities", "type": "Humanities", "dean": "Prof. Han"},
        {"id": "COL-BUS", "name": "School of Business", "type": "Business", "dean": "Prof. Park"},
        {"id": "COL-SCI", "name": "College of Natural Sciences", "type": "Science", "dean": "Prof. Jung"},
    ]

    departments = [
        {"id": "DEP-CSE", "name": "Computer Science and Engineering", "code": "CSE", "building": "ICT-101", "college_id": "COL-ICT"},
        {"id": "DEP-SWE", "name": "Software Engineering", "code": "SWE", "building": "ICT-102", "college_id": "COL-ICT"},
        {"id": "DEP-ICE", "name": "Information Communication Engineering", "code": "ICE", "building": "ICT-103", "college_id": "COL-ICT"},
        {"id": "DEP-EEE", "name": "Electrical Engineering", "code": "EEE", "building": "ENG-201", "college_id": "COL-ENG"},
        {"id": "DEP-ELE", "name": "Electronics Engineering", "code": "ELE", "building": "ENG-202", "college_id": "COL-ENG"},
        {"id": "DEP-MEC", "name": "Mechanical Engineering", "code": "MEC", "building": "ENG-301", "college_id": "COL-ENG"},
        {"id": "DEP-CVE", "name": "Civil Engineering", "code": "CVE", "building": "ENG-302", "college_id": "COL-ENG"},
        {"id": "DEP-CHE", "name": "Chemical Engineering", "code": "CHE", "building": "ENG-303", "college_id": "COL-ENG"},
        {"id": "DEP-IND", "name": "Industrial Engineering", "code": "IND", "building": "ENG-304", "college_id": "COL-ENG"},
        {"id": "DEP-BUS", "name": "Business Administration", "code": "BUS", "building": "BUS-101", "college_id": "COL-BUS"},
        {"id": "DEP-ECO", "name": "Global Economics", "code": "ECO", "building": "BUS-102", "college_id": "COL-BUS"},
        {"id": "DEP-TRD", "name": "International Trade", "code": "TRD", "building": "BUS-103", "college_id": "COL-BUS"},
        {"id": "DEP-ACC", "name": "Accounting", "code": "ACC", "building": "BUS-104", "college_id": "COL-BUS"},
        {"id": "DEP-ENG", "name": "English Language and Literature", "code": "ENG", "building": "HUM-101", "college_id": "COL-HUM"},
        {"id": "DEP-KOR", "name": "Korean Literature", "code": "KOR", "building": "HUM-102", "college_id": "COL-HUM"},
        {"id": "DEP-HIS", "name": "History", "code": "HIS", "building": "HUM-103", "college_id": "COL-HUM"},
        {"id": "DEP-MAT", "name": "Mathematics", "code": "MAT", "building": "SCI-101", "college_id": "COL-SCI"},
        {"id": "DEP-PHY", "name": "Physics", "code": "PHY", "building": "SCI-102", "college_id": "COL-SCI"},
        {"id": "DEP-CHM", "name": "Chemistry", "code": "CHM", "building": "SCI-103", "college_id": "COL-SCI"},
        {"id": "DEP-STA", "name": "Statistics", "code": "STA", "building": "SCI-104", "college_id": "COL-SCI"},
    ]

    track_topics = [
        "AI Systems",
        "Data Science",
        "Software Innovation",
        "Smart Factory",
        "Robotics",
        "Cyber Security",
        "IoT Platforms",
        "Digital Finance",
        "Marketing Analytics",
        "Global Commerce",
        "Cultural Content",
        "Bioinformatics",
        "Quantum Technology",
        "Green Energy",
        "Applied Mathematics",
    ]
    major_tracks: List[Dict[str, Any]] = []
    track_id_counter = 1
    for dept in departments:
        topic = track_topics[(track_id_counter - 1) % len(track_topics)]
        min_year = random.randint(1, 2)
        max_year = random.randint(min_year + 1, 4)
        major_tracks.append(
            {
                "id": f"TRK-{track_id_counter:03}",
                "name": f"{dept['code']} {topic}",
                "department_id": dept["id"],
                "focusArea": topic,
                "minYear": min_year,
                "maxYear": max_year,
            }
        )
        track_id_counter += 1
    dept_cycle = cycle(departments)
    while len(major_tracks) < 30:
        dept = next(dept_cycle)
        topic = random.choice(track_topics)
        min_year = random.randint(1, 3)
        max_year = random.randint(min_year, 4)
        major_tracks.append(
            {
                "id": f"TRK-{track_id_counter:03}",
                "name": f"{dept['code']} {topic}",
                "department_id": dept["id"],
                "focusArea": topic,
                "minYear": min_year,
                "maxYear": max_year,
            }
        )
        track_id_counter += 1

    terms: List[Dict[str, Any]] = []
    calendar_year = 2024
    for idx in range(8):
        half = 1 if idx % 2 == 0 else 2
        term_name = f"{calendar_year}-{half}"
        terms.append(
            {
                "id": f"TERM-{term_name}",
                "name": term_name,
                "year": calendar_year,
                "season": "Spring" if half == 1 else "Fall",
                "sequence": idx + 1,
                "yearBand": (idx // 2) + 1,
                "startDate": f"{calendar_year}-{'03' if half == 1 else '09'}-01",
                "endDate": f"{calendar_year}-{'06' if half == 1 else '12'}-30",
            }
        )
        if half == 2:
            calendar_year += 1

    event_templates = [
        ("Course Registration", 1, 2, None, "REG"),
        ("Add/Drop", 3, 4, None, "ADD"),
        ("Midterm Exams", 7, 8, None, "MID"),
        ("Final Exams", 15, 16, None, "FIN"),
        ("Graduation Check", 17, 18, 4, "GRD"),
    ]
    events: List[Dict[str, Any]] = []
    for term in terms:
        for name, start_week, end_week, fixed_year, abbr in event_templates:
            year_focus = fixed_year if fixed_year else random.randint(1, 4)
            events.append(
                {
                    "id": f"EVT-{term['id']}-{abbr}",
                    "name": f"{term['name']} {name}",
                    "termId": term["id"],
                    "eventType": name,
                    "startWeek": start_week,
                    "endWeek": end_week,
                    "yearFocus": year_focus,
                }
            )

    last_names = ["Kim", "Lee", "Park", "Choi", "Jung", "Kang", "Cho", "Yoon", "Jang", "Han"]
    first_names = ["Minji", "Jisoo", "Hana", "Sujin", "Hyeri", "Doyeon", "Yuna", "Seojun", "Hyunwoo", "Jiwon", "Taehyun", "Minseok"]
    book_topics = ["AI", "Networks", "Databases", "Robotics", "Economics", "Marketing", "Finance", "Literature", "Mathematics", "Chemistry"]
    publishers = ["CBNU Press", "Orion Publishing", "Campus House", "Scholarly Hub"]
    books: List[Dict[str, Any]] = []
    for idx in range(500):
        topic = random.choice(book_topics)
        title = f"{topic} Insights Vol {idx % 25 + 1}"
        author = f"{random.choice(last_names)} {random.choice(first_names)}"
        books.append(
            {
                "id": f"BOOK-{idx:04}",
                "name": title,
                "title": title,
                "author": author,
                "topic": topic,
                "available": random.random() > 0.25,
                "callNumber": f"{topic[:3].upper()}-{idx:04}",
                "publisher": random.choice(publishers),
            }
        )

    program_categories = ["Leadership", "AI", "Global", "Career", "Research", "Startup"]
    competencies = ["Creativity", "Collaboration", "Problem Solving", "Communication", "Global Citizenship", "Digital Literacy"]
    events_by_year: Dict[int, List[Dict[str, Any]]] = {}
    for event in events:
        events_by_year.setdefault(event["yearFocus"], []).append(event)
    programs: List[Dict[str, Any]] = []
    program_id_counter = 0

    def build_program(track: Dict[str, Any]) -> Dict[str, Any]:
        nonlocal program_id_counter
        min_year = random.randint(1, 3)
        max_year = random.randint(min_year, 4)
        event_candidates = [e for year in range(min_year, max_year + 1) for e in events_by_year.get(year, [])]
        if not event_candidates:
            event_candidates = events
        selected_events = random.sample(event_candidates, k=min(2, len(event_candidates)))
        track_ids = [track["id"]]
        if random.random() < 0.4:
            extra_track = random.choice(major_tracks)["id"]
            if extra_track not in track_ids:
                track_ids.append(extra_track)
        program = {
            "id": f"PRG-{program_id_counter:03}",
            "name": f"{random.choice(program_categories)} Program {program_id_counter:02}",
            "category": random.choice(program_categories),
            "competency": random.choice(competencies),
            "minYear": min_year,
            "maxYear": max_year,
            "hours": random.randint(12, 40),
            "delivery": random.choice(["Online", "On-site", "Hybrid"]),
            "track_ids": track_ids,
            "departmentId": track["department_id"],
            "event_ids": [evt["id"] for evt in selected_events],
        }
        program_id_counter += 1
        return program

    for track in major_tracks:
        programs.append(build_program(track))
    while len(programs) < 100:
        programs.append(build_program(random.choice(major_tracks)))

    professors: List[Dict[str, Any]] = []
    professor_id = 0
    for dept in departments:
        for _ in range(4):
            professor_id += 1
            name = f"{random.choice(last_names)} {random.choice(first_names)}"
            professors.append(
                {
                    "id": f"PROF-{professor_id:03}",
                    "name": name,
                    "title": random.choice(["Assistant Professor", "Associate Professor", "Professor"]),
                    "email": f"{name.lower().replace(' ', '.')}.{professor_id}@cbnu.ac.kr",
                    "office": f"{dept['code']}-{random.randint(200, 450)}",
                    "departmentId": dept["id"],
                }
            )
    professors_by_dept: Dict[str, List[Dict[str, Any]]] = {}
    for prof in professors:
        professors_by_dept.setdefault(prof["departmentId"], []).append(prof)

    programs_by_dept: Dict[str, List[str]] = {}
    programs_by_track: Dict[str, List[str]] = {}
    for program in programs:
        programs_by_dept.setdefault(program["departmentId"], []).append(program["id"])
        for track_id in program["track_ids"]:
            programs_by_track.setdefault(track_id, []).append(program["id"])
    program_lookup = {p["id"]: p for p in programs}

    course_topics = ["Algorithms", "Data Structures", "Media", "Operations", "Finance", "Sensors", "Networks", "Machine Learning", "Chemistry Lab", "Modern Poetry"]
    courses: List[Dict[str, Any]] = []
    course_book_pairs: List[Dict[str, str]] = []
    course_program_pairs: List[Dict[str, str]] = []
    course_prereq_pairs: List[Dict[str, str]] = []
    course_professor_pairs: List[Dict[str, str]] = []
    course_term_pairs: List[Dict[str, str]] = []
    course_lookup: Dict[str, Dict[str, Any]] = {}
    dept_courses: Dict[str, List[str]] = {}
    events_by_term: Dict[str, List[str]] = {}
    for event in events:
        events_by_term.setdefault(event["termId"], []).append(event["id"])
    course_counter = 0
    book_ids = [b["id"] for b in books]
    for dept in departments:
        for _ in range(12):
            prof = random.choice(professors_by_dept[dept["id"]])
            term = terms[(course_counter + len(courses)) % len(terms)]
            course_id = f"COURSE-{course_counter:04}"
            course_counter += 1
            topic = random.choice(course_topics)
            credits = random.choice([2, 3, 3, 4])
            course = {
                "id": course_id,
                "name": f"{topic} for {dept['code']} {random.randint(1, 4)}",
                "courseCode": f"{dept['code']}{100 + (course_counter % 400)}",
                "credits": credits,
                "category": random.choice(["core", "elective"]),
                "yearLevel": random.randint(1, 4),
                "semester": term["season"],
                "deliveryMode": random.choice(["In-person", "Blended", "Online"]),
                "departmentId": dept["id"],
                "termId": term["id"],
            }
            courses.append(course)
            course_lookup[course_id] = course
            dept_courses.setdefault(dept["id"], []).append(course_id)
            course_professor_pairs.append({"course_id": course_id, "professor_id": prof["id"]})
            course_term_pairs.append({"course_id": course_id, "term_id": term["id"]})

            if len(dept_courses[dept["id"]]) > 2 and random.random() < 0.5:
                prereq = random.choice(dept_courses[dept["id"]][:-1])
                course_prereq_pairs.append({"course_id": course_id, "prereq_id": prereq})

            rec_books = random.sample(book_ids, k=random.randint(1, 3))
            for book_id in rec_books:
                course_book_pairs.append({"course_id": course_id, "book_id": book_id})

            program_choices = programs_by_dept.get(dept["id"]) or [random.choice(programs)["id"]]
            selected_programs = random.sample(
                program_choices, k=min(len(program_choices), random.randint(1, 2))
            )
            for program_id in selected_programs:
                course_program_pairs.append({"course_id": course_id, "program_id": program_id})
            course_lookup[course_id]["program_ids"] = selected_programs

    while len(courses) < 260:
        dept = random.choice(departments)
        prof = random.choice(professors_by_dept[dept["id"]])
        term = random.choice(terms)
        course_id = f"COURSE-{course_counter:04}"
        course_counter += 1
        course = {
            "id": course_id,
            "name": f"Advanced {random.choice(course_topics)} {course_counter}",
            "courseCode": f"{dept['code']}{100 + (course_counter % 400)}",
            "credits": random.choice([3, 4]),
            "category": random.choice(["core", "elective"]),
            "yearLevel": random.randint(2, 4),
            "semester": term["season"],
            "deliveryMode": random.choice(["In-person", "Blended"]),
            "departmentId": dept["id"],
            "termId": term["id"],
        }
        courses.append(course)
        course_lookup[course_id] = course
        dept_courses.setdefault(dept["id"], []).append(course_id)
        course_professor_pairs.append({"course_id": course_id, "professor_id": prof["id"]})
        course_term_pairs.append({"course_id": course_id, "term_id": term["id"]})
        rec_books = random.sample(book_ids, k=random.randint(1, 3))
        for book_id in rec_books:
            course_book_pairs.append({"course_id": course_id, "book_id": book_id})
        program_choices = programs_by_dept.get(dept["id"]) or [random.choice(programs)["id"]]
        selected_programs = random.sample(program_choices, k=1)
        for program_id in selected_programs:
            course_program_pairs.append({"course_id": course_id, "program_id": program_id})
        course_lookup[course_id]["program_ids"] = selected_programs
        if len(dept_courses[dept["id"]]) > 2:
            prereq = random.choice(dept_courses[dept["id"]][:-1])
            course_prereq_pairs.append({"course_id": course_id, "prereq_id": prereq})

    event_course_pairs: List[Dict[str, str]] = []
    for course in courses:
        event_candidates = events_by_term.get(course["termId"], [])
        if not event_candidates:
            continue
        selected_events = random.sample(event_candidates, k=min(2, len(event_candidates)))
        for event_id in selected_events:
            event_course_pairs.append({"event_id": event_id, "course_id": course["id"]})

    scholarships: List[Dict[str, Any]] = []
    scholarship_course_pairs: List[Dict[str, str]] = []
    scholarship_program_pairs: List[Dict[str, str]] = []
    scholarship_track_pairs: List[Dict[str, str]] = []
    scholarship_term_pairs: List[Dict[str, str]] = []
    for idx in range(50):
        track_sample = random.sample(major_tracks, k=random.randint(1, 3))
        program_sample = random.sample(programs, k=random.randint(1, 2))
        course_sample = random.sample(courses, k=random.randint(1, 3))
        term_sample = random.sample(terms, k=random.randint(1, 2))
        min_year = random.randint(1, 3)
        max_year = random.randint(min_year, 4)
        scholarship = {
            "id": f"SCH-{idx:03}",
            "name": f"{random.choice(['Merit', 'Global', 'Innovation', 'Future'])} Scholarship {idx:02}",
            "category": random.choice(["Merit", "Need-based", "Research", "Global"]),
            "minGpa": round(random.uniform(2.7, 3.9), 2),
            "minCredits": random.choice([30, 45, 60, 90, 120]),
            "amount": random.choice([500000, 800000, 1000000, 1500000]),
            "targetYearMin": min_year,
            "targetYearMax": max_year,
            "status": random.choice(["open", "closed"]),
            "available_track_ids": [t["id"] for t in track_sample],
            "required_program_ids": [p["id"] for p in program_sample],
            "required_course_ids": [c["id"] for c in course_sample],
            "available_term_ids": [t["id"] for t in term_sample],
        }
        scholarships.append(scholarship)
        for prog_id in scholarship["required_program_ids"]:
            scholarship_program_pairs.append({"scholarship_id": scholarship["id"], "program_id": prog_id})
        for course_id in scholarship["required_course_ids"]:
            scholarship_course_pairs.append({"scholarship_id": scholarship["id"], "course_id": course_id})
        for track in scholarship["available_track_ids"]:
            scholarship_track_pairs.append({"scholarship_id": scholarship["id"], "track_id": track})
        for term in scholarship["available_term_ids"]:
            scholarship_term_pairs.append({"scholarship_id": scholarship["id"], "term_id": term})

    scholarships_by_track: Dict[str, List[Dict[str, Any]]] = {}
    for sch in scholarships:
        for track_id in sch["available_track_ids"]:
            scholarships_by_track.setdefault(track_id, []).append(sch)

    terms_by_band: Dict[int, List[Dict[str, Any]]] = {}
    for term in terms:
        terms_by_band.setdefault(term["yearBand"], []).append(term)

    students: List[Dict[str, Any]] = []
    student_track_pairs: List[Dict[str, str]] = []
    student_course_pairs: List[Dict[str, str]] = []
    student_program_pairs: List[Dict[str, Any]] = []
    student_scholarship_pairs: List[Dict[str, Any]] = []
    NUM_STUDENTS = 5600
    for idx in range(NUM_STUDENTS):
        track = random.choice(major_tracks)
        year_level = random.choices([1, 2, 3, 4], weights=[0.27, 0.26, 0.24, 0.23])[0]
        term_candidates = terms_by_band.get(year_level, terms)
        current_term = random.choice(term_candidates)
        status = "graduating" if year_level == 4 and random.random() < 0.35 else "active"
        credits = random.randint(year_level * 25, year_level * 35)
        if status == "graduating":
            credits = max(credits, 120 + random.randint(0, 25))
        gpa = round(random.uniform(2.0, 4.3), 2)
        student = {
            "id": f"STD-{idx:05}",
            "name": f"{random.choice(last_names)} {random.choice(first_names)}",
            "studentNumber": 20180000 + idx,
            "yearLevel": year_level,
            "gpa": gpa,
            "entryYear": random.randint(2018, 2023),
            "creditsEarned": credits,
            "requiredCredits": 130,
            "status": status,
            "currentTermId": current_term["id"],
        }
        students.append(student)
        student_track_pairs.append({"student_id": student["id"], "track_id": track["id"]})

        dept_course_list = dept_courses.get(track["department_id"], list(course_lookup.keys()))
        if not dept_course_list:
            dept_course_list = list(course_lookup.keys())
        num_courses = random.randint(4, 6)
        term_specific = [cid for cid in dept_course_list if course_lookup[cid]["termId"] == current_term["id"]]
        chosen_courses = set(random.sample(term_specific, k=min(len(term_specific), 2))) if term_specific else set()
        while len(chosen_courses) < num_courses:
            chosen_courses.add(random.choice(dept_course_list))
        for course_id in chosen_courses:
            student_course_pairs.append({"student_id": student["id"], "course_id": course_id})

        program_candidates = programs_by_track.get(track["id"], [])
        if program_candidates:
            participation = random.sample(program_candidates, k=min(len(program_candidates), random.randint(1, 3)))
            for program_id in participation:
                hours = min(program_lookup[program_id]["hours"], random.randint(8, 20))
                student_program_pairs.append({"student_id": student["id"], "program_id": program_id, "hours": hours})

        possible_scholarships = [
            sch
            for sch in scholarships_by_track.get(track["id"], [])
            if sch["targetYearMin"] <= year_level <= sch["targetYearMax"] and gpa >= sch["minGpa"] - 0.1
        ]
        if possible_scholarships and random.random() < 0.22:
            sch = random.choice(possible_scholarships)
            student_scholarship_pairs.append(
                {
                    "student_id": student["id"],
                    "scholarship_id": sch["id"],
                    "year": current_term["name"],
                }
            )

    department_college_pairs = [{"dept_id": dept["id"], "college_id": dept["college_id"]} for dept in departments]
    track_department_pairs = [{"track_id": track["id"], "dept_id": track["department_id"]} for track in major_tracks]
    program_track_pairs = [
        {"program_id": program["id"], "track_id": track_id} for program in programs for track_id in program["track_ids"]
    ]
    program_event_pairs = [
        {"program_id": program["id"], "event_id": event_id, "yearFocus": program["minYear"]}
        for program in programs
        for event_id in program["event_ids"]
    ]

    try:
        with driver.session() as session:
            run_batch(
                session,
                """
                UNWIND $rows AS row
                CREATE (:College {id: row.id, name: row.name, type: row.type, dean: row.dean})
                """,
                colleges,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                CREATE (:Department {id: row.id, name: row.name, code: row.code, building: row.building})
                """,
                departments,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                CREATE (:MajorTrack:AcademicInfo {
                    id: row.id,
                    name: row.name,
                    focusArea: row.focusArea,
                    minYear: row.minYear,
                    maxYear: row.maxYear,
                    departmentId: row.department_id
                })
                """,
                major_tracks,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                CREATE (:Term:AcademicInfo {
                    id: row.id,
                    name: row.name,
                    year: row.year,
                    season: row.season,
                    sequence: row.sequence,
                    startDate: row.startDate,
                    endDate: row.endDate,
                    yearBand: row.yearBand
                })
                """,
                terms,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                CREATE (:AcademicEvent:AcademicInfo {
                    id: row.id,
                    name: row.name,
                    eventType: row.eventType,
                    termId: row.termId,
                    startWeek: row.startWeek,
                    endWeek: row.endWeek,
                    yearFocus: row.yearFocus
                })
                """,
                events,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                CREATE (:Book:ScholarlyResource {
                    id: row.id,
                    name: row.name,
                    title: row.title,
                    author: row.author,
                    topic: row.topic,
                    available: row.available,
                    callNumber: row.callNumber,
                    publisher: row.publisher
                })
                """,
                books,
                batch_size=500,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                CREATE (:NonCurricularProgram:AcademicInfo {
                    id: row.id,
                    name: row.name,
                    category: row.category,
                    competency: row.competency,
                    minYear: row.minYear,
                    maxYear: row.maxYear,
                    hours: row.hours,
                    delivery: row.delivery,
                    departmentId: row.departmentId
                })
                """,
                programs,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                CREATE (:Professor:AcademicActor {
                    id: row.id,
                    name: row.name,
                    title: row.title,
                    email: row.email,
                    office: row.office,
                    departmentId: row.departmentId
                })
                """,
                professors,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                CREATE (:Course:AcademicInfo {
                    id: row.id,
                    name: row.name,
                    courseCode: row.courseCode,
                    credits: row.credits,
                    category: row.category,
                    yearLevel: row.yearLevel,
                    semester: row.semester,
                    deliveryMode: row.deliveryMode,
                    departmentId: row.departmentId,
                    termId: row.termId
                })
                """,
                courses,
                batch_size=500,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                CREATE (:Scholarship:AcademicInfo {
                    id: row.id,
                    name: row.name,
                    category: row.category,
                    minGpa: row.minGpa,
                    minCredits: row.minCredits,
                    amount: row.amount,
                    targetYearMin: row.targetYearMin,
                    targetYearMax: row.targetYearMax,
                    status: row.status
                })
                """,
                scholarships,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                CREATE (:Student:AcademicActor {
                    id: row.id,
                    name: row.name,
                    studentNumber: row.studentNumber,
                    yearLevel: row.yearLevel,
                    gpa: row.gpa,
                    entryYear: row.entryYear,
                    creditsEarned: row.creditsEarned,
                    requiredCredits: row.requiredCredits,
                    status: row.status,
                    currentTermId: row.currentTermId
                })
                """,
                students,
                batch_size=1000,
            )

            run_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (d:Department {id: row.dept_id})
                MATCH (c:College {id: row.college_id})
                CREATE (d)-[:BELONGS_TO]->(c)
                """,
                department_college_pairs,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (t:MajorTrack {id: row.track_id})
                MATCH (d:Department {id: row.dept_id})
                CREATE (t)-[:BELONGS_TO]->(d)
                """,
                track_department_pairs,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (p:NonCurricularProgram {id: row.program_id})
                MATCH (t:MajorTrack {id: row.track_id})
                CREATE (p)-[:SUITABLE_FOR_MAJOR]->(t)
                """,
                program_track_pairs,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (p:NonCurricularProgram {id: row.program_id})
                MATCH (e:AcademicEvent {id: row.event_id})
                CREATE (p)-[:SUITABLE_FOR_YEAR {targetYear: row.yearFocus}]->(e)
                """,
                program_event_pairs,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (c:Course {id: row.course_id})
                MATCH (p:Professor {id: row.professor_id})
                CREATE (c)-[:TAUGHT_BY]->(p)
                """,
                course_professor_pairs,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (c:Course {id: row.course_id})
                MATCH (t:Term {id: row.term_id})
                CREATE (c)-[:HELD_IN_TERM]->(t)
                """,
                course_term_pairs,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (c:Course {id: row.course_id})
                MATCH (b:Book {id: row.book_id})
                CREATE (c)-[:HAS_RECOMMENDED_BOOK]->(b)
                """,
                course_book_pairs,
                batch_size=1000,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (c:Course {id: row.course_id})
                MATCH (p:NonCurricularProgram {id: row.program_id})
                CREATE (c)-[:RELATED_TO_PROGRAM]->(p)
                """,
                course_program_pairs,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (c1:Course {id: row.course_id})
                MATCH (c2:Course {id: row.prereq_id})
                CREATE (c1)-[:HAS_PREREQUISITE]->(c2)
                """,
                course_prereq_pairs,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (sch:Scholarship {id: row.scholarship_id})
                MATCH (prog:NonCurricularProgram {id: row.program_id})
                CREATE (sch)-[:REQUIRES_PROGRAM]->(prog)
                """,
                scholarship_program_pairs,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (sch:Scholarship {id: row.scholarship_id})
                MATCH (course:Course {id: row.course_id})
                CREATE (sch)-[:REQUIRES_COURSE]->(course)
                """,
                scholarship_course_pairs,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (sch:Scholarship {id: row.scholarship_id})
                MATCH (track:MajorTrack {id: row.track_id})
                CREATE (sch)-[:AVAILABLE_FOR_MAJOR]->(track)
                """,
                scholarship_track_pairs,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (sch:Scholarship {id: row.scholarship_id})
                MATCH (term:Term {id: row.term_id})
                CREATE (sch)-[:AVAILABLE_IN_TERM]->(term)
                """,
                scholarship_term_pairs,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (s:Student {id: row.student_id})
                MATCH (t:MajorTrack {id: row.track_id})
                CREATE (s)-[:MAJOR_IN]->(t)
                """,
                student_track_pairs,
                batch_size=1000,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (s:Student {id: row.student_id})
                MATCH (c:Course {id: row.course_id})
                CREATE (s)-[:ENROLLED_IN]->(c)
                """,
                student_course_pairs,
                batch_size=1000,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (s:Student {id: row.student_id})
                MATCH (p:NonCurricularProgram {id: row.program_id})
                CREATE (s)-[:PARTICIPATED_IN {hours: row.hours}]->(p)
                """,
                student_program_pairs,
                batch_size=1000,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (s:Student {id: row.student_id})
                MATCH (sch:Scholarship {id: row.scholarship_id})
                CREATE (s)-[:RECEIVED_SCHOLARSHIP {term: row.year}]->(sch)
                """,
                student_scholarship_pairs,
            )
            run_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (e:AcademicEvent {id: row.event_id})
                MATCH (c:Course {id: row.course_id})
                CREATE (e)-[:RELATED_TO_COURSE]->(c)
                """,
                event_course_pairs,
            )

        print(
            f"Loaded sample data: {len(students)} students, {len(courses)} courses, "
            f"{len(programs)} programs, {len(scholarships)} scholarships."
        )
    except Exception as exc:  # pragma: no cover - setup helper
        raise RuntimeError("Failed to load sample data") from exc


if __name__ == "__main__":
    DRIVER = get_driver()
    try:
        clear_database(DRIVER)
        create_schema(DRIVER)
        load_sample_data(DRIVER)
        print("Sample data loaded. Execute pytest to run query validations.")
    finally:
        DRIVER.close()
