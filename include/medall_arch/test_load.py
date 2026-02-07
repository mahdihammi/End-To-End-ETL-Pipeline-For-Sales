import sys
from pathlib import Path

# Go up 2 levels to reach project root (DE-Airflow-Minio)
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from include.helpers.sql_helper import load_sql

sql_query = load_sql("bronze_table.sql")
print(sql_query)