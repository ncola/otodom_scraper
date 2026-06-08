import psycopg2
import os
import logging
from dotenv import load_dotenv

load_dotenv()


def get_db_connection():
    connection = None
    try:
        host = os.getenv('DB_HOST')
        dbname = os.getenv('DB_NAME')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        port = os.getenv('DB_PORT')
        sslmode = os.getenv("DB_SSLMODE", "require")

        if not all([host, dbname, user, password, port]):
            raise ValueError("missing required env variables for db connection")

        connection = psycopg2.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password,
            port=port,
            sslmode=sslmode
        )

        logging.debug(f"connected to database: {os.getenv('DB_NAME')}")
        return connection
    except Exception as error:
        logging.error(f"error connecting to database: {error}")
    return connection


def get_fresh_connection():
    conn = get_db_connection()
    if conn is None:
        raise RuntimeError("Connection to the database failed")
    return conn, conn.cursor()
