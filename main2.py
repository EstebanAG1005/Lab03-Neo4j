from neo4j import GraphDatabase
import random
import time

# Configura tus credenciales de Neo4j Aura
neo4j_uri = "neo4j+s://your-uri:port"
neo4j_user = "your-username"
neo4j_password = "your-password"


class Neo4jConnection:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def add_user(self, name, user_id):
        with self.driver.session() as session:
            session.write_transaction(add_user_tx, name, user_id)

    def add_movie(self, title, movie_id, year, plot):
        with self.driver.session() as session:
            session.write_transaction(add_movie_tx, title, movie_id, year, plot)

    def add_rating(self, user_id, movie_id, rating, timestamp):
        with self.driver.session() as session:
            session.write_transaction(
                add_rating_tx, user_id, movie_id, rating, timestamp
            )


def add_user_tx(tx, name, user_id):
    tx.run(
        "CREATE (u:User {name: $name, user_id: $user_id})", name=name, user_id=user_id
    )


def add_movie_tx(tx, title, movie_id, year, plot):
    tx.run(
        "CREATE (m:Movie {title: $title, movie_id: $movie_id, year: $year, plot: $plot})",
        title=title,
        movie_id=movie_id,
        year=year,
        plot=plot,
    )


def add_rating_tx(tx, user_id, movie_id, rating, timestamp):
    query = """
    MATCH (u:User {user_id: $user_id})
    MATCH (m:Movie {movie_id: $movie_id})
    CREATE (u)-[r:Rated {rating: $rating, timestamp: $timestamp}]->(m)
    """
    tx.run(
        query, user_id=user_id, movie_id=movie_id, rating=rating, timestamp=timestamp
    )


def populate_graph(conn):
    users = [
        ("Alice", "u1"),
        ("Bob", "u2"),
        ("Carol", "u3"),
        ("David", "u4"),
        ("Eve", "u5"),
    ]
    movies = [
        ("Movie A", 1, 2000, "Plot A"),
        ("Movie B", 2, 2001, "Plot B"),
        ("Movie C", 3, 2002, "Plot C"),
    ]

    for name, user_id in users:
        conn.add_user(name, user_id)

    for title, movie_id, year, plot in movies:
        conn.add_movie(title, movie_id, year, plot)

    for user_id in [u[1] for u in users]:
        rated_movies = random.sample(movies, 2)
        for movie in rated_movies:
            conn.add_rating(user_id, movie[1], random.randint(0, 5), int(time.time()))


# Ejemplo de uso
with Neo4jConnection(neo4j_uri, neo4j_user, neo4j_password) as conn:
    populate_graph(conn)
