import psycopg2, os, logging
from dotenv import load_dotenv


# Wymagany zainstalowany PostgreSQL oraz utworzona baza apartments_for_sale
# Utworzenie bazy:
# psql -U postgres
# CREATE DATABASE apartments_for_sale;


# załadowanie zmiennych środowiskowych z .env
load_dotenv()


# ustawienie połączenia z bazą danych 
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
        connection = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        logging.debug(f"Connected to database {os.getenv('DB_NAME')}")
        return connection
    except Exception as error:
        logging.debug(f"Error while connecting to database: {error}")
    return connection


# Utworzenie tabel
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
        conn = get_db_connection()
        if conn is None:
            logging.error("Connection to the databas failed")
            return
    
        cur=conn.cursor()

        # Sprawdź czy tabele juz nie istnieja
        def check_table_exists(cur, table_name):
            checking_query = """
                SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s);
                """
            cur.execute(checking_query, (table_name,)) #argumenty w zapytaniach muszą byv przekazane jako krotka lub lista
            result = cur.fetchone()
            return result[0] # true/false
        
        tables = ['locations', 'apartments_sale_listings', 'price_history', 'photos', 'features']
        
        flag = any(check_table_exists(cur, name) for name in tables)
        if not flag:
            try:
                with open('db/schema.sql', 'r') as f:
                    sql_script = f.read()
                
                sql_commands = sql_script.split(";")
                for command in sql_commands:
                    if command.strip():
                        cur.execute(command.strip())
                
                conn.commit()
                logging.info(f"Tables {tables} created")
            except Exception as error:
                logging.exception(f"Error during creating tables: {error}")

    except Exception as error:
        logging.exception(f"Error during creating tables in database: {error}")

