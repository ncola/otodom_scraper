import psycopg2, os, logging
from dotenv import load_dotenv


# requires PostgreSQL running and a database named apartments_for_sale_otodom
# to create it manually:
# psql -U postgres
# CREATE DATABASE apartments_for_sale_otodom;

load_dotenv()


def get_db_connection():
    """
    Establishes a connection to the PostgreSQL database using credentials from environment variables
    
    Reads the database connection settings from the environment variables
    (DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT) and uses them to establish a 
    connection to the PostgreSQL 

    Returns:
        connection: psycopg2 connection object, or None if connection fails

    Raises:
        Exception: If there is an error during connection setup
    """

    connection = None
    try:
        host=os.getenv('DB_HOST')
        dbname=os.getenv('DB_NAME')
        user=os.getenv('DB_USER')
        password=os.getenv('DB_PASSWORD')
        port=os.getenv('DB_PORT')
        sslmode=os.getenv("DB_SSLMODE", "require")

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


def check_table_exists(cur, table_name):
            checking_query = """
                SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s);
                """
            cur.execute(checking_query, (table_name,))
            result = cur.fetchone()
            return result[0]  # True/False


def create_tables(cur):
    """
    Creates the necessary tables for storing apartment listings, price history, photos, and features in the database

    This function checks if the required tables (`locations`, `apartments_sale_listings`, `price_history`,
    `photos`, `features`) already exist in the database. If they do not exist, it reads SQL commands from a
    file (db/schema.sql) and executes them to create the tables

    The schema.sql file should contain the SQL scripts for creating these tables

    Raises:
        Exception: If an error occurs during the table creation process
    """
    try:
        tables = ['locations', 'apartments_sale_listings', 'price_history', 'photos', 'features']

        flag = all(check_table_exists(cur, name) for name in tables)
        if not flag:
            try:
                schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
                with open(schema_path, 'r') as f:
                    sql_script = f.read()

                sql_commands = sql_script.split(";")
                for command in sql_commands:
                    if command.strip():
                        cur.execute(command.strip())

                cur.connection.commit()
                logging.info(f"tables created: {tables}")
            except Exception as error:
                logging.exception(f"error creating tables: {error}")

    except Exception as error:
        logging.exception(f"error during table setup: {error}")

