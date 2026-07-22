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
        apartment_tables = ['locations', 'apartments_sale_listings', 'price_history', 'photos', 'features']
        plot_tables = ['plots_sale_listings', 'plots_price_history']
        apartments_ready = all(check_table_exists(cur, name) for name in apartment_tables)
        plots_ready = all(check_table_exists(cur, name) for name in plot_tables)

        if not apartments_ready or not plots_ready:
            try:
                schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
                with open(schema_path, 'r') as f:
                    sql_script = f.read()

                # Existing installations often already have the apartment
                # schema.  In that case execute only the idempotent PLOTS
                # section instead of trying to create apartment tables again.
                if apartments_ready and not plots_ready:
                    sql_script = sql_script.split('-- PLOTS', maxsplit=1)[1]

                sql_commands = sql_script.split(";")
                for command in sql_commands:
                    if command.strip():
                        cur.execute(command.strip())

                cur.connection.commit()
                logging.info("database tables created or completed")
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
