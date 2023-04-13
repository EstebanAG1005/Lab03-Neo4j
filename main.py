from neo4j import GraphDatabase
import random
import time

# Configura tus credenciales de Neo4j Aura
neo4j_uri = "neo4j+s://2789fbed.databases.neo4j.io"
neo4j_user = "neo4j"
neo4j_password = "3Iv0-YJvjzQw37igxGuKwLdlz5XQio5Y3RfQsnzbuXA"

# Inicia la conexión a Neo4j
driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

# Funciones para crear nodos y relaciones en Neo4j
def add_user(tx, name, user_id):
    tx.run(
        "CREATE (u:User {name: $name, user_id: $user_id})", name=name, user_id=user_id
    )


def add_movie(tx, title, movie_id, year, plot):
    tx.run(
        "CREATE (m:Movie {title: $title, movie_id: $movie_id, year: $year, plot: $plot})",
        title=title,
        movie_id=movie_id,
        year=year,
        plot=plot,
    )


def add_rating(tx, user_id, movie_id, rating, timestamp):
    query = """
    MATCH (u:User {user_id: $user_id})
    MATCH (m:Movie {movie_id: $movie_id})
    CREATE (u)-[r:Rated {rating: $rating, timestamp: $timestamp}]->(m)
    """
    tx.run(
        query, user_id=user_id, movie_id=movie_id, rating=rating, timestamp=timestamp
    )


# Poblar grafo en Neo4j
def populate_graph(driver):
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

    with driver.session() as session:
        # session.write_transaction(add_user, "Esteban", 1002)
        # session.write_transaction(add_movie, "Maverick", 4, 2023, "Plot D")

        # session.write_transaction(
        #     add_rating,
        #     1002,
        #     4,
        #     random.randint(0, 5),
        #     int(time.time()),
        # )

        for name, user_id in users:
            session.write_transaction(add_user, name, user_id)

        for title, movie_id, year, plot in movies:
            session.write_transaction(add_movie, title, movie_id, year, plot)

        for user_id in [u[1] for u in users]:
            rated_movies = random.sample(movies, 2)
            for movie in rated_movies:
                session.write_transaction(
                    add_rating,
                    user_id,
                    movie[1],
                    random.randint(0, 5),
                    int(time.time()),
                )


def find_user(tx, user_id):
    result = tx.run("MATCH (u:User {user_id: $user_id}) RETURN u", user_id=user_id)
    record = result.single()
    if record:
        return record["u"]
    else:
        return None


def find_movie(tx, movie_id):
    result = tx.run("MATCH (m:Movie {movie_id: $movie_id}) RETURN m", movie_id=movie_id)
    record = result.single()
    if record:
        return record["m"]
    else:
        return None


def find_user_rating_for_movie(tx, user_id, movie_id):
    query = """
    MATCH (u:User {user_id: $user_id})-[r:Rated]->(m:Movie {movie_id: $movie_id})
    RETURN u, r, m
    """
    result = tx.run(query, user_id=user_id, movie_id=movie_id)
    record = result.single()
    if record:
        return {
            "user": record["u"],
            "rating": record["r"]["rating"],
            "timestamp": record["r"]["timestamp"],
            "movie": record["m"],
        }
    else:
        return None


# populate_graph(driver)
def print_results(user, movie, user_rating):
    print("User found:")
    print("  Name: {0}".format(user["name"]))
    print("  User ID: {0}".format(user["user_id"]))
    print()

    print("Movie found:")
    print("  Title: {0}".format(movie["title"]))
    print("  Movie ID: {0}".format(movie["movie_id"]))
    print("  Year: {0}".format(movie["year"]))
    print("  Plot: {0}".format(movie["plot"]))
    print()

    print("User rating for movie:")
    print("  User: {0}".format(user_rating["user"]["name"]))
    print("  Movie: {0}".format(user_rating["movie"]["title"]))
    print("  Rating: {0}".format(user_rating["rating"]))
    print("  Timestamp: {0}".format(user_rating["timestamp"]))
    print()


if __name__ == "__main__":
    with driver.session() as session:
        user = session.read_transaction(find_user, "u2")
        movie = session.read_transaction(find_movie, 1)
        user_rating = session.read_transaction(find_user_rating_for_movie, "u2", 3)

        print_results(user, movie, user_rating)
