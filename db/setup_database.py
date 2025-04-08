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
    try:
        connection = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            dbname=os.getenv('DB_NAME'),
            user=os.getnev('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        print(f"Connected with database {os.getenv('DB_NAME')}")
        return connection
    except Exception as error:
        print(error)
    finally:
        connection.close()



# Utworzenie tabel (in progress, checking variables)
conn = None
cur = None
def create_tables():
    try:
        conn = get_db_connection()

        cur=conn.cursor()

        #table: offers
        create_script = '''CREATE TABLE IF NOT EXISTS offers (
                            id                  SERIAL PRIMARY KEY,
                            title               TEXT NOT NULL,
                            market              VARCHAR(10),
                            advert_type         VARCHAR(8)),
                            description_text    TEXT;'''
        
        cur.execute(create_script)

        #table: history_of_offers

        #table: photos

        
        conn.commit()
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
    














"""
conn = None
cur = None
try:
    conn = psycopg2.connect(
            host='localhost',
            dbname='test',
            user='postgres',
            password='haslo123',
            port=5432
        )

    cur=conn.cursor()

    cur.execute('DROP TABLE IF EXISTS employee')

    create_script = '''CREATE TABLE IF NOT EXISTS employee (
                        id  int PRIMARY KEY,
                        name    varchar(40) NOT NULL,
                        salary  int,
                        dept_id varchar(30));'''
    
    cur.execute(create_script)

    insert_script = '''INSERT INTO employee (id, name, salary, dept_id) VALUES (%s, %s, %s, %s)'''
    insert_values = (1, 'James', 12000, 'D1'), (2, 'Robin', 12000, 'D1'), (3, 'Olaf', 12000, 'D1'), (4, 'Xavier', 20000, 'D2')
    
    for record in insert_values:
        cur.execute(insert_script, record)


    cur.execute('SELECT * FROM EMPLOYEE')
    print(cur.fetchall())

    cur.execute('SELECT * FROM EMPLOYEE')
    for record in cur.fetchall():
        print(record)


    conn.commit()
except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()"""