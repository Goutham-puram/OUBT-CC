#!/usr/bin/env python3
"""
Load NYC Taxi Data into RDS PostgreSQL Star Schema
Loads 10,000 sample records into the star schema tables.
"""

import logging
import sys
import json
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.extensions import connection as PGConnection

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.de_intern_2024.utils.logger import setup_logger

# Configure logging
logger = setup_logger(__name__)


class RDSDataLoader:
    """Load NYC taxi data into RDS PostgreSQL star schema."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str
    ):
        """
        Initialize data loader.

        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn: Optional[PGConnection] = None
        self.df: Optional[pd.DataFrame] = None

        self.stats = {
            'dim_location': 0,
            'dim_time': 0,
            'dim_rate': 0,
            'fact_trips': 0
        }

    def connect(self) -> PGConnection:
        """
        Connect to RDS PostgreSQL database.

        Returns:
            Database connection

        Raises:
            Exception: If connection fails
        """
        try:
            logger.info(f"Connecting to database {self.database} at {self.host}:{self.port}")

            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=30
            )

            # Set autocommit to False for transaction management
            self.conn.autocommit = False

            logger.info("Database connection established")

            # Test connection
            cur = self.conn.cursor()
            cur.execute("SELECT version();")
            version = cur.fetchone()
            logger.info(f"PostgreSQL version: {version[0]}")
            cur.close()

            return self.conn

        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def disconnect(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

    def test_schema_exists(self) -> bool:
        """
        Test if star schema tables exist.

        Returns:
            True if all tables exist, False otherwise
        """
        try:
            cur = self.conn.cursor()

            # Check for required tables
            required_tables = ['dim_location', 'dim_time', 'dim_rate', 'fact_trips']

            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE'
            """)

            existing_tables = [row[0] for row in cur.fetchall()]
            cur.close()

            missing_tables = [t for t in required_tables if t not in existing_tables]

            if missing_tables:
                logger.error(f"Missing tables: {', '.join(missing_tables)}")
                logger.info("Please run the schema creation script first:")
                logger.info("psql -h HOST -U USER -d DATABASE -f sql/rds/001_create_star_schema.sql")
                return False

            logger.info("All required tables exist")
            return True

        except Exception as e:
            logger.error(f"Failed to check schema: {e}")
            return False

    def load_sample_data(self, filepath: str = None, n_records: int = 10000) -> pd.DataFrame:
        """
        Load sample taxi data from file.

        Args:
            filepath: Path to sample data file
            n_records: Number of records to load

        Returns:
            DataFrame with sample data
        """
        if filepath is None:
            # Try to find sample data file
            script_dir = Path(__file__).parent
            data_dir = script_dir.parent.parent / 'data' / 'processed'

            # Try parquet first, then CSV
            parquet_file = data_dir / 'yellow_tripdata_2024-01_sample.parquet'
            csv_file = data_dir / 'yellow_tripdata_2024-01_sample_rds.csv'
            cleaned_file = data_dir / 'yellow_tripdata_2024-01_cleaned.parquet'

            if parquet_file.exists():
                filepath = parquet_file
            elif csv_file.exists():
                filepath = csv_file
            elif cleaned_file.exists():
                filepath = cleaned_file
                logger.info("Using cleaned data file, will sample from it")
            else:
                raise FileNotFoundError(
                    "No sample data file found. Please run sample_taxi_data.py first."
                )

        filepath = Path(filepath)
        logger.info(f"Loading data from {filepath}")

        try:
            if filepath.suffix == '.parquet':
                self.df = pd.read_parquet(filepath)
            elif filepath.suffix == '.csv':
                self.df = pd.read_csv(filepath)
            else:
                raise ValueError(f"Unsupported file format: {filepath.suffix}")

            # Sample if needed
            if len(self.df) > n_records:
                logger.info(f"Sampling {n_records} records from {len(self.df)} total")
                self.df = self.df.sample(n=n_records, random_state=42)
            else:
                logger.info(f"Using all {len(self.df)} records")

            logger.info(f"Loaded {len(self.df)} records")
            logger.info(f"Columns: {', '.join(self.df.columns)}")

            return self.df

        except Exception as e:
            logger.error(f"Failed to load sample data: {e}")
            raise

    def populate_dim_location(self):
        """
        Populate location dimension table with unique locations from data.
        Note: This uses simplified location data. In production, you would
        load from the official NYC TLC taxi zone lookup table.
        """
        try:
            logger.info("Populating dim_location...")

            # Get unique location IDs from data
            pickup_ids = self.df['PULocationID'].dropna().unique()
            dropoff_ids = self.df['DOLocationID'].dropna().unique()
            all_location_ids = set(pickup_ids) | set(dropoff_ids)

            cur = self.conn.cursor()

            # Get existing locations
            cur.execute("SELECT location_id FROM dim_location")
            existing_locations = {row[0] for row in cur.fetchall()}

            # Find new locations that need to be added
            new_locations = all_location_ids - existing_locations

            if new_locations:
                logger.info(f"Adding {len(new_locations)} new locations")

                # Insert new locations with placeholder data
                # In production, you would join with the actual taxi zone lookup table
                insert_query = """
                    INSERT INTO dim_location (location_id, borough, zone, service_zone)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (location_id) DO NOTHING
                """

                data = [
                    (int(loc_id), 'Unknown', f'Location {int(loc_id)}', 'Unknown')
                    for loc_id in new_locations
                ]

                execute_batch(cur, insert_query, data, page_size=1000)
                self.stats['dim_location'] = len(new_locations)

                logger.info(f"Inserted {len(new_locations)} location records")
            else:
                logger.info("All locations already exist in dim_location")

            cur.close()

        except Exception as e:
            logger.error(f"Failed to populate dim_location: {e}")
            raise

    def populate_dim_time(self):
        """
        Populate time dimension table with unique timestamps from data.
        """
        try:
            logger.info("Populating dim_time...")

            # Ensure datetime column is parsed
            if 'tpep_pickup_datetime' not in self.df.columns:
                raise ValueError("Column 'tpep_pickup_datetime' not found in data")

            # Convert to datetime if needed
            if not pd.api.types.is_datetime64_any_dtype(self.df['tpep_pickup_datetime']):
                self.df['tpep_pickup_datetime'] = pd.to_datetime(self.df['tpep_pickup_datetime'])

            # Get unique timestamps
            unique_timestamps = self.df['tpep_pickup_datetime'].dropna().unique()
            logger.info(f"Found {len(unique_timestamps)} unique timestamps")

            cur = self.conn.cursor()

            # Use the insert_or_get_time_id function for each timestamp
            # This automatically handles deduplication
            insert_query = """
                INSERT INTO dim_time (
                    pickup_datetime, year, month, day, hour, weekday,
                    is_weekend, quarter, day_of_year, week_of_year
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pickup_datetime) DO NOTHING
            """

            data = []
            for ts in unique_timestamps:
                dt = pd.Timestamp(ts)
                data.append((
                    dt.to_pydatetime(),
                    dt.year,
                    dt.month,
                    dt.day,
                    dt.hour,
                    dt.dayofweek,
                    dt.dayofweek in [5, 6],  # Saturday or Sunday
                    dt.quarter,
                    dt.dayofyear,
                    dt.isocalendar()[1]  # ISO week number
                ))

            execute_batch(cur, insert_query, data, page_size=1000)
            self.stats['dim_time'] = len(data)

            logger.info(f"Inserted {len(data)} time records")

            cur.close()

        except Exception as e:
            logger.error(f"Failed to populate dim_time: {e}")
            raise

    def populate_fact_trips(self):
        """
        Populate fact table with trip data.
        """
        try:
            logger.info("Populating fact_trips...")

            # Prepare data for insertion
            cur = self.conn.cursor()

            # Build insert query
            insert_query = """
                INSERT INTO fact_trips (
                    time_id,
                    pickup_location_id,
                    dropoff_location_id,
                    rate_code_id,
                    passenger_count,
                    trip_distance,
                    trip_duration,
                    fare_amount,
                    extra,
                    mta_tax,
                    tip_amount,
                    tolls_amount,
                    improvement_surcharge,
                    congestion_surcharge,
                    total_amount,
                    payment_type
                )
                SELECT
                    dt.time_id,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                FROM dim_time dt
                WHERE dt.pickup_datetime = %s
                LIMIT 1
            """

            # Prepare data
            data = []
            skipped = 0

            for idx, row in self.df.iterrows():
                try:
                    # Extract values with proper type handling
                    pickup_loc = int(row['PULocationID']) if pd.notna(row['PULocationID']) else None
                    dropoff_loc = int(row['DOLocationID']) if pd.notna(row['DOLocationID']) else None
                    rate_code = int(row.get('RatecodeID', 1)) if pd.notna(row.get('RatecodeID')) else 1

                    # Skip if required fields are missing
                    if pickup_loc is None or dropoff_loc is None:
                        skipped += 1
                        continue

                    # Ensure rate_code is valid (1-6)
                    if rate_code not in [1, 2, 3, 4, 5, 6]:
                        rate_code = 1  # Default to standard rate

                    # Calculate trip duration if available
                    trip_duration = None
                    if 'trip_duration' in row and pd.notna(row['trip_duration']):
                        trip_duration = float(row['trip_duration'])
                    elif 'tpep_dropoff_datetime' in self.df.columns:
                        if pd.notna(row['tpep_dropoff_datetime']) and pd.notna(row['tpep_pickup_datetime']):
                            pickup = pd.Timestamp(row['tpep_pickup_datetime'])
                            dropoff = pd.Timestamp(row['tpep_dropoff_datetime'])
                            trip_duration = (dropoff - pickup).total_seconds() / 60.0

                    # Extract numeric values with defaults
                    passenger_count = int(row.get('passenger_count', 1)) if pd.notna(row.get('passenger_count')) else 1
                    trip_distance = float(row.get('trip_distance', 0)) if pd.notna(row.get('trip_distance')) else 0
                    fare_amount = float(row.get('fare_amount', 0)) if pd.notna(row.get('fare_amount')) else 0
                    extra = float(row.get('extra', 0)) if pd.notna(row.get('extra')) else 0
                    mta_tax = float(row.get('mta_tax', 0)) if pd.notna(row.get('mta_tax')) else 0
                    tip_amount = float(row.get('tip_amount', 0)) if pd.notna(row.get('tip_amount')) else 0
                    tolls_amount = float(row.get('tolls_amount', 0)) if pd.notna(row.get('tolls_amount')) else 0
                    improvement_surcharge = float(row.get('improvement_surcharge', 0)) if pd.notna(row.get('improvement_surcharge')) else 0
                    congestion_surcharge = float(row.get('congestion_surcharge', 0)) if pd.notna(row.get('congestion_surcharge')) else 0
                    total_amount = float(row.get('total_amount', 0)) if pd.notna(row.get('total_amount')) else 0
                    payment_type = int(row.get('payment_type', 1)) if pd.notna(row.get('payment_type')) else 1

                    # Get pickup datetime
                    pickup_datetime = pd.Timestamp(row['tpep_pickup_datetime']).to_pydatetime()

                    data.append((
                        pickup_loc,
                        dropoff_loc,
                        rate_code,
                        passenger_count,
                        trip_distance,
                        trip_duration,
                        fare_amount,
                        extra,
                        mta_tax,
                        tip_amount,
                        tolls_amount,
                        improvement_surcharge,
                        congestion_surcharge,
                        total_amount,
                        payment_type,
                        pickup_datetime
                    ))

                except Exception as e:
                    logger.debug(f"Skipping row {idx}: {e}")
                    skipped += 1
                    continue

            # Execute batch insert
            logger.info(f"Inserting {len(data)} trip records...")
            if skipped > 0:
                logger.warning(f"Skipped {skipped} rows due to missing/invalid data")

            execute_batch(cur, insert_query, data, page_size=500)
            self.stats['fact_trips'] = len(data)

            logger.info(f"Inserted {len(data)} trip records")

            cur.close()

        except Exception as e:
            logger.error(f"Failed to populate fact_trips: {e}")
            raise

    def validate_data_load(self) -> Dict[str, int]:
        """
        Validate data load by checking record counts.

        Returns:
            Dictionary with table counts
        """
        try:
            logger.info("Validating data load...")

            cur = self.conn.cursor()

            tables = ['dim_location', 'dim_time', 'dim_rate', 'fact_trips']
            counts = {}

            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                counts[table] = count
                logger.info(f"  {table}: {count:,} records")

            cur.close()

            return counts

        except Exception as e:
            logger.error(f"Failed to validate data load: {e}")
            raise

    def run_sample_queries(self):
        """Run sample analytical queries to verify data."""
        try:
            logger.info("\nRunning sample queries...")

            cur = self.conn.cursor()

            # Query 1: Top pickup locations
            logger.info("\nTop 5 pickup locations:")
            cur.execute("""
                SELECT
                    pl.zone,
                    pl.borough,
                    COUNT(*) as trip_count,
                    ROUND(AVG(ft.total_amount), 2) as avg_fare
                FROM fact_trips ft
                JOIN dim_location pl ON ft.pickup_location_id = pl.location_id
                GROUP BY pl.zone, pl.borough
                ORDER BY trip_count DESC
                LIMIT 5
            """)

            for row in cur.fetchall():
                logger.info(f"  {row[0]} ({row[1]}): {row[2]} trips, ${row[3]} avg fare")

            # Query 2: Trips by hour
            logger.info("\nTrips by hour of day:")
            cur.execute("""
                SELECT
                    dt.hour,
                    COUNT(*) as trip_count,
                    ROUND(AVG(ft.total_amount), 2) as avg_fare
                FROM fact_trips ft
                JOIN dim_time dt ON ft.time_id = dt.time_id
                GROUP BY dt.hour
                ORDER BY dt.hour
                LIMIT 5
            """)

            for row in cur.fetchall():
                logger.info(f"  Hour {row[0]:02d}: {row[1]} trips, ${row[2]} avg fare")

            # Query 3: Weekend vs weekday
            logger.info("\nWeekend vs Weekday:")
            cur.execute("""
                SELECT
                    CASE WHEN dt.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
                    COUNT(*) as trip_count,
                    ROUND(AVG(ft.total_amount), 2) as avg_fare,
                    ROUND(AVG(ft.trip_distance), 2) as avg_distance
                FROM fact_trips ft
                JOIN dim_time dt ON ft.time_id = dt.time_id
                GROUP BY dt.is_weekend
            """)

            for row in cur.fetchall():
                logger.info(f"  {row[0]}: {row[1]} trips, ${row[2]} avg fare, {row[3]} miles avg")

            cur.close()

        except Exception as e:
            logger.error(f"Failed to run sample queries: {e}")
            raise

    def load_data(self, filepath: str = None, n_records: int = 10000) -> bool:
        """
        Main method to load data into RDS.

        Args:
            filepath: Path to sample data file
            n_records: Number of records to load

        Returns:
            True if successful, False otherwise
        """
        try:
            # Connect to database
            self.connect()

            # Check if schema exists
            if not self.test_schema_exists():
                logger.error("Schema does not exist. Cannot proceed.")
                return False

            # Load sample data
            self.load_sample_data(filepath, n_records)

            # Start transaction
            logger.info("Starting data load transaction...")

            try:
                # Populate dimension tables
                self.populate_dim_location()
                self.populate_dim_time()
                # dim_rate is already populated by schema script

                # Populate fact table
                self.populate_fact_trips()

                # Validate
                counts = self.validate_data_load()

                # Commit transaction
                self.conn.commit()
                logger.info("Transaction committed successfully!")

                # Run sample queries
                self.run_sample_queries()

                # Print summary
                self.print_summary()

                return True

            except Exception as e:
                logger.error(f"Error during data load: {e}")
                logger.info("Rolling back transaction...")
                self.conn.rollback()
                logger.info("Transaction rolled back")
                raise

        except Exception as e:
            logger.error(f"Data load failed: {e}")
            return False

        finally:
            self.disconnect()

    def print_summary(self):
        """Print summary of data load."""
        print("\n" + "="*80)
        print("DATA LOAD SUMMARY")
        print("="*80)
        print(f"Source records:     {len(self.df):,}")
        print(f"dim_location:       {self.stats['dim_location']:,} records inserted")
        print(f"dim_time:           {self.stats['dim_time']:,} records inserted")
        print(f"fact_trips:         {self.stats['fact_trips']:,} records inserted")
        print("="*80 + "\n")


def load_connection_info(config_file: str = None) -> Dict[str, Any]:
    """
    Load connection information from config file.

    Args:
        config_file: Path to connection config file

    Returns:
        Dictionary with connection parameters
    """
    if config_file is None:
        script_dir = Path(__file__).parent
        config_file = script_dir.parent.parent / 'config' / 'rds_connection.json'

    config_file = Path(config_file)

    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_file}")

    with open(config_file, 'r') as f:
        config = json.load(f)

    return config


def main():
    """Main execution function."""
    import argparse
    import getpass

    parser = argparse.ArgumentParser(description='Load NYC taxi data into RDS PostgreSQL')
    parser.add_argument('--host', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--database', default='oubt_ptg', help='Database name')
    parser.add_argument('--user', help='Database user')
    parser.add_argument('--password', help='Database password')
    parser.add_argument('--config', help='Path to connection config JSON file')
    parser.add_argument('--data-file', help='Path to sample data file')
    parser.add_argument('--records', type=int, default=10000, help='Number of records to load')

    args = parser.parse_args()

    try:
        # Load connection info from config or arguments
        if args.config or (not args.host and not args.user):
            logger.info("Loading connection info from config file...")
            config = load_connection_info(args.config)
            host = args.host or config['host']
            port = args.port or config['port']
            database = args.database or config['database']
            user = args.user or config['username']
        else:
            host = args.host
            port = args.port
            database = args.database
            user = args.user

        # Get password
        password = args.password
        if not password:
            password = getpass.getpass(f"Enter password for {user}@{host}: ")

        # Create loader
        loader = RDSDataLoader(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )

        # Load data
        logger.info("Starting data load process...")
        logger.info(f"Target: {database} at {host}:{port}")
        logger.info(f"Records to load: {args.records:,}")

        success = loader.load_data(args.data_file, args.records)

        if success:
            logger.info("Data load completed successfully!")
            sys.exit(0)
        else:
            logger.error("Data load failed!")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.warning("\nOperation cancelled by user")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
