from loguru import logger
import psycopg2
from dotenv import load_dotenv
import os
from faker import Faker
import random
import time

load_dotenv()

NUM_RECORDS = 10_000

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


def fill_people_table(conn):
    with conn.cursor() as cur:
        start = time.time()
        faker = Faker()

        # Insert random data
        for _ in range(NUM_RECORDS):
            name = faker.name()
            age = random.randint(18, 90)
            city = faker.city()
            email = faker.email()
            is_active = random.choice([True, False])

            cur.execute(
                "INSERT INTO people (name, age, city, email, is_active) VALUES (%s, %s, %s, %s, %s)",
                (name, age, city, email, is_active)
            )

        conn.commit()
        end = time.time()
        logger.info(f"Inserted {NUM_RECORDS} records in {end - start:.2f} seconds.")


def main():
    logger.info(os.getenv("db_name"))


        # Connect to the PostgreSQL database using environment variables directly
    with psycopg2.connect(
            host=os.getenv("DB_HOST"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            port=os.getenv("DB_PORT")
        ) as conn:

        create_sample_table(conn)
        fill_people_table(conn)




if __name__ == '__main__':
    main()