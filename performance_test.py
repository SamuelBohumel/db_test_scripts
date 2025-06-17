from loguru import logger
import psycopg2
from dotenv import load_dotenv
import os
from faker import Faker
import random
import time
import pandas as pd

load_dotenv()

NUM_RECORDS = 1_000_000
query_count = 10_000

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


def create_sample_dataset(file_name):
    faker = Faker()
    data = []
    for _ in range(NUM_RECORDS):
        name = faker.name()
        age = random.randint(18, 90)
        city = faker.city()
        email = faker.email()
        is_active = random.choice([True, False])
        data.append((name, age, city, email, is_active))

    # Create DataFrame
    df = pd.DataFrame(data, columns=["name", "age", "city", "email", "is_active"])

    # Save DataFrame to CSV file
    df.to_csv(file_name, index=False)


def fill_people_table_copy(conn, file_name):
    start = time.time()

    with conn.cursor() as cur:
        with open(file_name, 'r', encoding='utf-8') as f:
            next(f)  # Skip header line
            cur.copy_expert(
                "COPY people(name, age, city, email, is_active) FROM STDIN WITH CSV",f
            )

    conn.commit()
    end = time.time()
    logger.info(f"Inserted records from {file_name} using COPY in {end - start:.2f} seconds.")


def fill_people_table_inserts(conn, file_name):
    start = time.time()

    with conn.cursor() as cur:
        with open(file_name, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)  # Skip header row

            for row in reader:
                name, age, city, email, is_active = row
                cur.execute(
                    """
                    INSERT INTO people (name, age, city, email, is_active)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (name, int(age), city, email, is_active.lower() == 'true')
                )

    conn.commit()
    end = time.time()
    logger.info(f"Inserted records from {file_name} using individual INSERTs in {end - start:.2f} seconds.")


def create_index(conn):
    with conn.cursor() as cur:
        # Create index on city column
        logger.info("Creating index on 'city' column...")
        cur.execute("CREATE INDEX idx_city ON people(city)")
        conn.commit()


def delete_index(conn):
    with conn.cursor() as cur:
        # Create index on city column
        logger.info("Creating index on 'city' column...")
        cur.execute("DROP INDEX idx_city;")
        conn.commit()


def query_performance_test(conn, file_name):
    with conn.cursor() as cur:
        df = pd.read_csv(file_name)
        cities = df['city'].sample(n=query_count, random_state=42).tolist()

        # Run 100 queries with random cities (pre-index)
        logger.info(f"Running {query_count} SELECT queries without index...")
        start = time.time()
        for city in cities:
            cur.execute("SELECT * FROM people WHERE city = %s", (city,))
            cur.fetchall()
        end = time.time()
        duration = end - start
        logger.info(f"Total time: {(duration):.5f} seconds")
        return duration


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

        # create_sample_table(conn)
        # fill_people_table_copy(conn, file_name)
        select_time = query_performance_test(conn, file_name)
        logger.info(f"Select test: {select_time:.5f} seconds, average time per query: {(select_time / query_count):.5f} seconds")



if __name__ == '__main__':
    main()