"""
Run a series of queries on the Neo4j database and show SQL equivalent
"""
import os

import polars as pl
from codetiming import Timer
from dotenv import load_dotenv
from neo4j import GraphDatabase, Session

load_dotenv()
# Config
URI = "bolt://localhost:7687"
NEO4J_USER = os.environ.get("NEO4J_USER")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")


def run_query1(session: Session, country: str) -> None:
    "Which 5 cities in a particular country have the lowest average age in the network?"
    query = """
        MATCH (p:Person) -[:LIVES_IN]-> (c:City) -[*1..2]-> (co:Country)
        WHERE co.country = $country
        RETURN c.city AS city, avg(p.age) AS averageAge
        ORDER BY averageAge LIMIT 5
    """
    sql_equivalent = """
        SELECT Cities.city_name AS city, AVG(Persons.age) AS averageAge
        FROM Persons
        JOIN Cities ON Persons.city_id = Cities.city_id
        JOIN States ON Cities.state_id = States.state_id
        JOIN Countries ON States.country_id = Countries.country_id
        WHERE Countries.country_name = ?
        GROUP BY Cities.city_name
        ORDER BY averageAge LIMIT 5;
    """
    print(f"\nQuery 1:\n {query}")
    print(f"\nEquivalent in relational DBMS:\n {sql_equivalent}")
    response = session.run(query, country=country)
    result = pl.from_dicts(response.data())
    print(f"Cities with lowest average age in {country}:\n{result}")
    return result


def run_query2(session: Session, age_lower: int, age_upper: int) -> None:
    "How many persons between a certain age range are in each country?"
    query = """
        MATCH (p:Person)-[:LIVES_IN]->(ci:City)-[*1..2]->(country:Country)
        WHERE p.age >= $age_lower AND p.age <= $age_upper
        RETURN country.country AS countries, count(country) AS personCounts
        ORDER BY personCounts DESC LIMIT 3
    """
    sql_equivalent = """
        SELECT Countries.country_name AS countries, COUNT(*) AS personCounts
        FROM Persons
        JOIN Cities ON Persons.city_id = Cities.city_id
        JOIN States ON Cities.state_id = States.state_id
        JOIN Countries ON States.country_id = Countries.country_id
        WHERE Persons.age BETWEEN ? AND ?
        GROUP BY Countries.country_name
        ORDER BY personCounts DESC LIMIT 3;
    """
    print(f"\nQuery 2:\n {query}")
    print(f"\nEquivalent in relational DBMS:\n {sql_equivalent}")
    response = session.run(query, age_lower=age_lower, age_upper=age_upper)
    result = pl.from_dicts(response.data())
    print(f"Persons between ages {age_lower}-{age_upper} in each country:\n{result}")
    return result


def run_query3(session: Session, gender: str, city: str, country: str, interest: str) -> None:
    "How many men in a particular city have an interest in the same thing?"
    query = """
        MATCH (p:Person)-[:HAS_INTEREST]->(i:Interest)
        WHERE tolower(i.interest) = tolower($interest)
        AND tolower(p.gender) = tolower($gender)
        WITH p, i
        MATCH (p)-[:LIVES_IN]->(c:City)
        WHERE c.city = $city AND c.country = $country
        RETURN count(p) AS numPersons
    """
    sql_equivalent = """
        SELECT COUNT(DISTINCT Persons.person_id) AS numPersons
        FROM Persons
        JOIN PersonInterests ON Persons.person_id = PersonInterests.person_id
        JOIN Interests ON PersonInterests.interest_id = Interests.interest_id
        JOIN Cities ON Persons.city_id = Cities.city_id
        WHERE 
            LOWER(Interests.interest) = LOWER(?)
            AND LOWER(Persons.gender) = LOWER(?)
            AND LOWER(Cities.city_name) = LOWER(?)
            AND LOWER(Cities.country) = LOWER(?);
    """
    print(f"\nQuery 3:\n {query}")
    response = session.run(query, gender=gender, city=city, country=country, interest=interest)
    result = pl.from_dicts(response.data())
    print(
        f"Number of {gender} users in {city}, {country} who have an interest in {interest}:\n{result}"
    )
    return result


def run_query4(
    session: Session, country: str, age_lower: int, age_upper: int, interest: str
) -> None:
    "Which U.S. state has the maximum number of persons between a specified age who enjoy a particular interest?"
    query = """
        MATCH (p:Person)-[:LIVES_IN]->(:City)-[:CITY_IN]->(s:State)
        WHERE p.age >= $age_lower AND p.age <= $age_upper AND s.country = $country
        WITH p, s
        MATCH (p)-[:HAS_INTEREST]->(i:Interest)
        WHERE tolower(i.interest) = tolower($interest)
        RETURN count(p) AS numPersons, s.state AS state, s.country AS country
        ORDER BY numPersons DESC LIMIT 1
    """
    sql_equivalent = """
        SELECT COUNT(*) AS numPersons, States.state_name AS state, States.country
        FROM Persons
        JOIN Cities ON Persons.city_id = Cities.city_id
        JOIN States ON Cities.state_id = States.state_id
        JOIN PersonInterests ON Persons.person_id = PersonInterests.person_id
        JOIN Interests ON PersonInterests.interest_id = Interests.interest_id
        WHERE Persons.age BETWEEN ? AND ? AND States.country = ?
            AND LOWER(Interests.interest) = LOWER(?)
        GROUP BY States.state_id
        ORDER BY numPersons DESC
        LIMIT 1;
    """
    print(f"\nQuery 4:\n {query}")
    print(f"\nEquivalent in relational DBMS:\n {sql_equivalent}")
    response = session.run(
        query, country=country, age_lower=age_lower, age_upper=age_upper, interest=interest
    )
    result = pl.from_dicts(response.data())
    print(
        f"""
        State in {country} with the most users between ages {age_lower}-{age_upper} who have an interest in {interest}:\n{result}
        """
    )
    return result


def run_query5(session: Session) -> None:
    "How many second-degree paths exist in the graph?"
    query = """
        MATCH (a:Person)-[r1:FOLLOWS]->(b:Person)-[r2:FOLLOWS]->(c:Person)
        RETURN count(*) AS numPaths
    """
    sql_equivalent = """
        SELECT COUNT(*) AS numPaths
        FROM Follows AS r1
        JOIN Follows AS r2 ON r1.followee_id = r2.follower_id;
    """
    print(f"\nQuery 5:\n {query}")
    print(f"\nEquivalent in relational DBMS:\n {sql_equivalent}")
    response = session.run(query)
    result = pl.from_dicts(response.data())
    print(
        f"""
        Number of second-degree paths:\n{result}
        """
    )
    return result


def main() -> None:
    with GraphDatabase.driver(URI, auth=(NEO4J_USER, NEO4J_PASSWORD)) as driver:
        with driver.session(database="neo4j") as session:
            with Timer(name="queries", text="Neo4j query script completed in {:.6f}s"):
                # fmt: off
                _ = run_query1(session, country="United States")
                _ = run_query2(session, age_lower=30, age_upper=40)
                _ = run_query3(session, gender="male", city="London", country="United Kingdom", interest="fine dining")
                _ = run_query4(session, country="United States", age_lower=23, age_upper=30, interest="photography")
                _ = run_query5(session)
                # fmt: on


if __name__ == "__main__":
    main()
