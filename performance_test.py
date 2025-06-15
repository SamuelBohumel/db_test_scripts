from loguru import logger
import psycopg2
from dotenv import load_dotenv
import os
from faker import Faker
import random
import time
import pandas as pd

load_dotenv()

NUM_RECORDS = 100_000

def create_sample_table(conn):
    cur = conn.cursor()

    # Create table
    cur.execute("""
        DROP TABLE IF EXISTS people;
        CREATE TABLE people (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            age INT,
            city VARCHAR(100),
            email VARCHAR(100),
            is_active BOOLEAN
        );
    """)
    conn.commit()
    logger.info("Table 'people' created.")


def fill_people_table(conn, file_name):
    with conn.cursor() as cur:
        start = time.time()
        faker = Faker()

        data = []
        # Insert random data
        for _ in range(NUM_RECORDS):
            name = faker.name()
            age = random.randint(18, 90)
            city = faker.city()
            email = faker.email()
            is_active = random.choice([True, False])

            data.append((name, age, city, email, is_active))
            cur.execute(
                "INSERT INTO people (name, age, city, email, is_active) VALUES (%s, %s, %s, %s, %s)",
                (name, age, city, email, is_active)
            )

        conn.commit()
        end = time.time()
        logger.info(f"Inserted {NUM_RECORDS} records in {end - start:.2f} seconds.")
        # Create DataFrame and save to CSV
        df = pd.DataFrame(data, columns=["name", "age", "city", "email", "is_active"])
        df.to_csv("people_data.csv", index=False)


def query_performance_test(conn, file_name):
    query_count = 1000
    with conn.cursor() as cur:
        df = pd.read_csv(file_name)
        cities = df['city'][:query_count]

        # Run 100 queries with random cities (pre-index)
        logger.info(f"Running {query_count} SELECT queries without index...")
        start = time.time()
        for city in cities:
            cur.execute("SELECT * FROM people WHERE city = %s", (city,))
            cur.fetchall()
        end = time.time()
        logger.info(f"Average query time (no index): {(end - start) / query_count:.5f} seconds")

        # Create index on city column
        logger.info("Creating index on 'city' column...")
        cur.execute("CREATE INDEX idx_city ON people(city)")
        conn.commit()

        # Run 100 queries with the same cities (post-index)
        logger.info(f"Running {query_count} SELECT queries with index...")
        start = time.time()
        for city in cities:
            cur.execute("SELECT * FROM people WHERE city = %s", (city,))
            cur.fetchall()
        end = time.time()
        logger.info(f"Average query time (with index): {(end - start) / query_count:.5f} seconds")
        cur.execute("DROP INDEX idx_city;")


def main():
    logger.info(os.getenv("db_name"))

    file_name = "people_data.csv"
        # Connect to the PostgreSQL database using environment variables directly
    with psycopg2.connect(
            host=os.getenv("DB_HOST"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            port=os.getenv("DB_PORT")
        ) as conn:

        create_sample_table(conn)
        fill_people_table(conn, file_name)
        query_performance_test(conn, file_name)



if __name__ == '__main__':
    main()