# include/helpers/sql_helper.py
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]  # include/

SQL_FOLDER = BASE_DIR / "sql"  # Remove / "all_sql_files"

def load_sql(file_name: str) -> str:
    """
    Load a SQL file from include/sql and return it as a string.
    
    Args:
        file_name (str): file name, e.g. 'dim_customer.sql'
    
    Returns:
        str: SQL content
    """
    file_path = SQL_FOLDER / file_name
    if not file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {file_path}")
    return file_path.read_text()