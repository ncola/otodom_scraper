import os
import logging


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
    return result[0]


def create_tables(cur):
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


def run_migrations(cur):
    cur.execute("""
        ALTER TABLE apartments_sale_listings
        ADD COLUMN IF NOT EXISTS db_created_at TIMESTAMP
        NOT NULL DEFAULT CURRENT_TIMESTAMP
    """)

    cur.execute("""
        ALTER TABLE apartments_sale_listings
        ADD COLUMN IF NOT EXISTS db_updated_at TIMESTAMP
        NOT NULL DEFAULT CURRENT_TIMESTAMP
    """)

    cur.execute("""
        ALTER TABLE price_history
        ADD COLUMN IF NOT EXISTS db_created_at TIMESTAMP
        NOT NULL DEFAULT CURRENT_TIMESTAMP
    """)
