"""RDS PostgreSQL database connector and utilities."""

from typing import Any, List, Optional, Dict
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import pandas as pd

from ..config import config
from ..utils.logger import get_logger

logger = get_logger(__name__)


class RDSConnector:
    """PostgreSQL RDS connection manager."""

    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize RDS connector.

        Args:
            connection_string: PostgreSQL connection string.
                              If None, uses config.
        """
        self.connection_string = connection_string or config.rds.connection_string
        self._engine: Optional[Engine] = None
        logger.info("RDSConnector initialized")

    @property
    def engine(self) -> Engine:
        """Get or create SQLAlchemy engine."""
        if self._engine is None:
            logger.info("Creating SQLAlchemy engine")
            self._engine = create_engine(self.connection_string)
        return self._engine

    def test_connection(self) -> bool:
        """
        Test database connection.

        Returns:
            True if connection successful, False otherwise.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            logger.info("Database connection successful")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False

    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            List of result rows as dictionaries.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                columns = result.keys()
                rows = [dict(zip(columns, row)) for row in result.fetchall()]
            logger.info(f"Query executed successfully, returned {len(rows)} rows")
            return rows
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def execute_statement(self, statement: str, params: Optional[Dict] = None) -> None:
        """
        Execute a non-SELECT statement (INSERT, UPDATE, DELETE, CREATE, etc.).

        Args:
            statement: SQL statement string
            params: Statement parameters
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(statement), params or {})
                conn.commit()
            logger.info("Statement executed successfully")
        except Exception as e:
            logger.error(f"Statement execution failed: {e}")
            raise

    def read_sql(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Execute a query and return results as a pandas DataFrame.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            DataFrame with query results.
        """
        try:
            df = pd.read_sql(text(query), self.engine, params=params or {})
            logger.info(f"Query returned {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Failed to read SQL: {e}")
            raise

    def write_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        index: bool = False
    ) -> None:
        """
        Write a pandas DataFrame to a database table.

        Args:
            df: DataFrame to write
            table_name: Target table name
            if_exists: Action if table exists ('fail', 'replace', 'append')
            index: Whether to write DataFrame index
        """
        try:
            df.to_sql(
                table_name,
                self.engine,
                if_exists=if_exists,
                index=index
            )
            logger.info(f"Wrote {len(df)} rows to table {table_name}")
        except Exception as e:
            logger.error(f"Failed to write DataFrame: {e}")
            raise

    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """
        Get information about a table's columns.

        Args:
            table_name: Name of the table

        Returns:
            DataFrame with column information.
        """
        query = """
        SELECT
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_name = :table_name
        ORDER BY ordinal_position
        """
        return self.read_sql(query, {"table_name": table_name})

    def close(self) -> None:
        """Close database connection."""
        if self._engine:
            self._engine.dispose()
            logger.info("Database connection closed")
