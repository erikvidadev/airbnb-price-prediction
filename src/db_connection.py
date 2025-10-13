"""
Minimal PostgreSQL database connection module for Jupyter/notebook usage.

Provides a simple interface to connect, write, and read pandas DataFrames.
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd

# 1️⃣ Load .env once at the start
load_dotenv()

class DatabaseConfig:
    """Loads DB settings from environment variables."""
    def __init__(self):
        self.user = os.getenv("DB_USER", "")
        self.password = os.getenv("DB_PASS", "")
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = os.getenv("DB_PORT", "5432")
        self.database = os.getenv("DB_NAME", "airbnb_db")

        if not self.user or not self.password:
            raise ValueError("Missing DB_USER or DB_PASS in .env file.")


class DatabaseConnection:
    """Simple database connection for read/write operations."""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.engine = self._create_engine()

    def _create_engine(self):
        """Create SQLAlchemy engine."""
        engine_url = (
            f"postgresql+psycopg2://{self.config.user}:{self.config.password}"
            f"@{self.config.host}:{self.config.port}/{self.config.database}"
        )
        return create_engine(engine_url)

    def write_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
        """Write DataFrame to SQL table."""
        df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
        print(f"DataFrame successfully saved to table: {table_name}")

    def read_dataframe(self, query: str) -> pd.DataFrame:
        """Read SQL query into DataFrame."""
        return pd.read_sql(query, self.engine)

    def test_connection(self) -> bool:
        """Optional: test DB connection."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("Database connection successful.")
            return True
        except Exception as e:
            print(f"Database connection failed: {e}")
            return False


def create_database_if_missing(config: DatabaseConfig):
    """Create the database if it doesn't exist (run once at setup)."""
    default_engine_url = (
        f"postgresql+psycopg2://{config.user}:{config.password}"
        f"@{config.host}:{config.port}/postgres"
    )
    engine = create_engine(default_engine_url)
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :dbname"),
            {"dbname": config.database}
        )
        if not result.scalar():
            conn.execute(text(f"CREATE DATABASE {config.database}"))
            print(f"Database '{config.database}' created.")
