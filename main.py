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


def add_genre(tx, name):
    tx.run("CREATE (g:Genre {name: $name})", name=name)


# Funciones para crear nodos y relaciones en Neo4j
def person_director(tx, name, tmdb_id, born, died, born_in, url, imdb_id, bio, poster):
    tx.run(
        "CREATE (pd:PersonDirector {name: $name, tmdb_id: $tmdb_id, born: $born, died: $died, born_in: $born_in, url: $url, imdb_id: $imdb_id, bio: $bio, poster: $poster})",
        name=name,
        tmdb_id=tmdb_id,
        born=born,
        died=died,
        born_in=born_in,
        url=url,
        imdb_id=imdb_id,
        bio=bio,
        poster=poster,
    )


def person_actor(tx, name, tmdb_id, born, died, born_in, url, imdb_id, bio, poster):
    tx.run(
        "CREATE (pa:PersonActor {name: $name, tmdb_id: $tmdb_id, born: $born, died: $died, born_in: $born_in, url: $url, imdb_id: $imdb_id, bio: $bio, poster: $poster})",
        name=name,
        tmdb_id=tmdb_id,
        born=born,
        died=died,
        born_in=born_in,
        url=url,
        imdb_id=imdb_id,
        bio=bio,
        poster=poster,
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


def add_directed(tx, imdb_id, movie_id, role):
    query = """
    MATCH (pd:PersonDirector {imdb_id: $imdb_id})
    MATCH (m:Movie {movie_id: $movie_id})
    CREATE (pd)-[r:Directed {role: $role}]->(m)
    """
    tx.run(query, imdb_id=imdb_id, movie_id=movie_id, role=role)


def add_acted_in(tx, imdb_id, movie_id, role):
    query = """
    MATCH (pa:PersonActor {imdb_id: $imdb_id})
    MATCH (m:Movie {movie_id: $movie_id})
    CREATE (pa)-[r:Acted_In {role: $role}]->(m)
    """
    tx.run(query, imdb_id=imdb_id, movie_id=movie_id, role=role)


def add_in_genre(tx, movie_id, name):
    query = """
    MATCH (m:Movie {movie_id: $movie_id})
    MATCH (g:Genre {name: $name})
    CREATE (m)-[r:In_Genre]->(g)
    """
    tx.run(query, name=name, movie_id=movie_id)


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

        # Crear nodos de ejemplo
        session.write_transaction(
            person_director,
            "Quentin Tarantino",
            138,
            "1963-03-27T00:00:00",
            None,
            "Knoxville, Tennessee, USA",
            "https://www.example.com",
            233,
            "Quentin Jerome Tarantino is an American filmmaker...",
            "https://image.example.com",
        )
        session.write_transaction(
            person_actor,
            "Leonardo DiCaprio",
            6193,
            "1974-11-11T00:00:00",
            None,
            "Los Angeles, California, USA",
            "https://www.example.com",
            138,
            "Leonardo Wilhelm DiCaprio is an American actor and film producer...",
            "https://image.example.com",
        )
        session.write_transaction(
            add_movie,
            "Inception",
            27205,
            "2010-07-16T00:00:00",
            8.8,
            1,
            2010,
            1375666,
            148,
            ["USA", "UK"],
            2002816,
            "https://www.example.com",
            825532764,
            "A thief who steals corporate secrets...",
            "https://image.example.com",
            160000000,
            ["English", "Japanese", "French"],
        )

        # Crear relaciones de ejemplo
        session.write_transaction(add_directed, 233, 1, "Director")
        session.write_transaction(add_acted_in, 138, 1, "Cobb")

        # Crear Genre
        session.write_transaction(add_genre, "Action")

        # Relacion de Movie y Genre
        session.write_transaction(add_in_genre, 1, "Action")
