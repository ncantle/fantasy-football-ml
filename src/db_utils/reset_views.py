import os
import logging
from sqlalchemy import create_engine, text

def apply_sql_scripts(engine, directory):
    logging.info(f"Applying SQL scripts from: {directory}")
    sql_files = sorted([
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if f.endswith('.sql')
    ])

    with engine.begin() as conn:
        for file_path in sql_files:
            logging.info(f"⏳ Running: {os.path.basename(file_path)}")
            with open(file_path, 'r') as f:
                sql = f.read()
                try:
                    conn.execute(text(sql))
                    logging.info(f"Success: {os.path.basename(file_path)}")
                except Exception as e:
                    logging.error(f"Error in {os.path.basename(file_path)}: {e}")
                    raise

def reset_views_and_indexes(engine):
    apply_sql_scripts(engine, "sql/views")
