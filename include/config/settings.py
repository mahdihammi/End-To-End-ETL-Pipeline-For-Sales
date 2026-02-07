from json import load
import os
from dotenv import load_dotenv


load_dotenv()
# Load environment variables from a .env file if present


RAW_BUCKET = os.getenv("RAW_BUCKET")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("SILVER_BUCKET")
GOLD_BUCKET = os.getenv("GOLD_BUCKET")

# # Optional safety check
# REQUIRED_VARS = [
#     RAW_BUCKET,
#     BRONZE_BUCKET,
# ]

# if not all(REQUIRED_VARS):
#     raise EnvironmentError("Missing required environment variables")


print("Configuration settings loaded successfully.")
print(f"RAW_BUCKET: {RAW_BUCKET}")