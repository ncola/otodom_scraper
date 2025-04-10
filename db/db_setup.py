import psycopg2, os
from dotenv import load_dotenv


# Wymagany zainstalowany PostgreSQL oraz utworzona baza apartments_for_sale
# Utworzenie bazy:
# psql -U postgres
# CREATE DATABASE apartments_for_sale;


# załadowanie zmiennych środowiskowych z .env
load_dotenv()


# ustawienie połączenia z bazą danych 
def get_db_connection():
    connection = None
    try:
        connection = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        #print(f"Connected to database {os.getenv('DB_NAME')}")
        return connection
    except Exception as error:
        print(f"Error while connecting to database: {error}")
    return connection



# Utworzenie tabel
def create_tables():
    conn = None
    cur = None  
    try:
        conn = get_db_connection()
        if conn is None:
            print("Connection to the databas failed")
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
                print("Tables created")
            except Exception as error:
                print(f"Error during creating tables: {error}")

    except Exception as error:
        print(f"Error 2: {error}")
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
    
