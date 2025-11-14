"""
Demo Data Generator for NYC Taxi Data Pipeline.

Generates synthetic March 2024 taxi trip data for demo purposes.
Ensures high data quality and realistic values for smooth demonstration.
"""

import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any
import random
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TaxiDataGenerator:
    """Generate synthetic NYC taxi trip data."""

    # NYC taxi zones (simplified - main areas)
    PICKUP_ZONES = [
        (40.7589, -73.9851),  # Times Square
        (40.7614, -73.9776),  # Grand Central
        (40.7484, -73.9857),  # Empire State Building
        (40.7282, -74.0776),  # Battery Park
        (40.7580, -73.9855),  # Theater District
        (40.7614, -73.9776),  # Midtown East
        (40.7549, -73.9840),  # Bryant Park
        (40.7489, -73.9680),  # UN Headquarters
        (40.7614, -73.9776),  # Rockefeller Center
        (40.7308, -73.9973),  # Greenwich Village
    ]

    DROPOFF_ZONES = [
        (40.7484, -73.9857),  # Empire State Building
        (40.7580, -73.9855),  # Theater District
        (40.7282, -74.0776),  # Battery Park
        (40.7589, -73.9851),  # Times Square
        (40.7614, -73.9776),  # Grand Central
        (40.7549, -73.9840),  # Bryant Park
        (40.7308, -73.9973),  # Greenwich Village
        (40.7614, -73.9776),  # Rockefeller Center
        (40.7489, -73.9680),  # UN Headquarters
        (40.7614, -73.9776),  # Midtown East
    ]

    PAYMENT_TYPES = {
        1: 0.60,  # Credit card (60%)
        2: 0.35,  # Cash (35%)
        3: 0.03,  # No charge (3%)
        4: 0.02,  # Dispute (2%)
    }

    RATE_CODES = {
        1: 0.92,  # Standard rate (92%)
        2: 0.03,  # JFK (3%)
        3: 0.02,  # Newark (2%)
        4: 0.02,  # Nassau or Westchester (2%)
        5: 0.01,  # Negotiated fare (1%)
    }

    def __init__(self, num_records: int = 10000, year: int = 2024, month: int = 3):
        """
        Initialize the taxi data generator.

        Args:
            num_records: Number of taxi trip records to generate
            year: Year for the data
            month: Month for the data (1-12)
        """
        self.num_records = num_records
        self.year = year
        self.month = month
        self.start_date = datetime(year, month, 1)

        # Calculate last day of the month
        if month == 12:
            self.end_date = datetime(year + 1, 1, 1) - timedelta(seconds=1)
        else:
            self.end_date = datetime(year, month + 1, 1) - timedelta(seconds=1)

        logger.info(
            f"Initialized generator for {num_records} records "
            f"from {self.start_date} to {self.end_date}"
        )

    def _generate_timestamp(self) -> datetime:
        """Generate a random timestamp within the month."""
        total_seconds = int((self.end_date - self.start_date).total_seconds())
        random_seconds = random.randint(0, total_seconds)

        # Add time-of-day patterns (more trips during rush hours)
        timestamp = self.start_date + timedelta(seconds=random_seconds)
        hour = timestamp.hour

        # Adjust for realistic patterns (peak hours: 7-9 AM, 5-7 PM)
        if hour in [7, 8, 17, 18]:
            # More likely to be during peak hours
            if random.random() < 0.3:  # 30% chance to regenerate
                return self._generate_timestamp()

        return timestamp

    def _calculate_fare_components(
        self, distance: float, duration_minutes: float
    ) -> Dict[str, float]:
        """
        Calculate realistic fare components.

        Args:
            distance: Trip distance in miles
            duration_minutes: Trip duration in minutes

        Returns:
            Dictionary with fare components
        """
        # Base fare
        base_fare = 2.50

        # Per mile charge (varies by time and distance)
        per_mile = 2.50 if distance > 0.2 else 0.50
        distance_fare = distance * per_mile

        # Time-based fare (for slow traffic)
        time_fare = (duration_minutes / 60) * 30.0 if duration_minutes > 5 else 0

        # Total fare (before extras)
        fare_amount = base_fare + distance_fare + time_fare

        # MTA tax
        mta_tax = 0.50

        # Tolls (10% chance)
        tolls = random.choice([0, 0, 0, 0, 0, 0, 0, 0, 0, 5.76])

        # Improvement surcharge
        improvement_surcharge = 0.30

        # Tip (usually 15-20% for credit cards, 0 for cash)
        tip_amount = 0  # Will be set based on payment type

        # Total amount
        total_amount = fare_amount + mta_tax + tolls + improvement_surcharge

        return {
            'fare_amount': round(fare_amount, 2),
            'mta_tax': mta_tax,
            'tip_amount': tip_amount,  # Placeholder
            'tolls_amount': tolls,
            'improvement_surcharge': improvement_surcharge,
            'total_amount': round(total_amount, 2)
        }

    def _generate_trip(self, trip_id: int) -> Dict[str, Any]:
        """
        Generate a single taxi trip record.

        Args:
            trip_id: Unique trip identifier

        Returns:
            Dictionary containing trip data
        """
        # Pickup and dropoff locations
        pickup_location = random.choice(self.PICKUP_ZONES)
        dropoff_location = random.choice(self.DROPOFF_ZONES)

        # Timestamps
        pickup_datetime = self._generate_timestamp()

        # Trip distance (0.5 to 20 miles, skewed toward shorter trips)
        distance = round(random.gammavariate(2, 1.5), 2)
        distance = min(max(distance, 0.5), 20.0)

        # Trip duration (based on distance + traffic)
        base_duration = (distance / 15) * 60  # Assume 15 mph average
        traffic_factor = random.uniform(0.8, 2.0)  # Traffic variability
        duration_minutes = base_duration * traffic_factor

        dropoff_datetime = pickup_datetime + timedelta(minutes=duration_minutes)

        # Passenger count (1-6, most commonly 1-2)
        passenger_count = random.choices([1, 2, 3, 4, 5, 6], weights=[0.6, 0.25, 0.08, 0.04, 0.02, 0.01])[0]

        # Payment type
        payment_type = random.choices(
            list(self.PAYMENT_TYPES.keys()),
            weights=list(self.PAYMENT_TYPES.values())
        )[0]

        # Rate code
        rate_code = random.choices(
            list(self.RATE_CODES.keys()),
            weights=list(self.RATE_CODES.values())
        )[0]

        # Calculate fare components
        fare_components = self._calculate_fare_components(distance, duration_minutes)

        # Add tip for credit card payments (15-20%)
        if payment_type == 1:  # Credit card
            tip_percentage = random.uniform(0.15, 0.22)
            fare_components['tip_amount'] = round(fare_components['fare_amount'] * tip_percentage, 2)
            fare_components['total_amount'] = round(
                fare_components['total_amount'] + fare_components['tip_amount'], 2
            )

        # Vendor ID (1 or 2)
        vendor_id = random.choice([1, 2])

        # Store and forward flag (rarely used)
        store_and_fwd_flag = 'N' if random.random() > 0.02 else 'Y'

        return {
            'VendorID': vendor_id,
            'tpep_pickup_datetime': pickup_datetime,
            'tpep_dropoff_datetime': dropoff_datetime,
            'passenger_count': passenger_count,
            'trip_distance': distance,
            'pickup_longitude': pickup_location[1],
            'pickup_latitude': pickup_location[0],
            'RatecodeID': rate_code,
            'store_and_fwd_flag': store_and_fwd_flag,
            'dropoff_longitude': dropoff_location[1],
            'dropoff_latitude': dropoff_location[0],
            'payment_type': payment_type,
            'fare_amount': fare_components['fare_amount'],
            'extra': 0.50 if pickup_datetime.hour >= 20 or pickup_datetime.hour <= 6 else 0.0,
            'mta_tax': fare_components['mta_tax'],
            'tip_amount': fare_components['tip_amount'],
            'tolls_amount': fare_components['tolls_amount'],
            'improvement_surcharge': fare_components['improvement_surcharge'],
            'total_amount': fare_components['total_amount'] + (0.50 if pickup_datetime.hour >= 20 or pickup_datetime.hour <= 6 else 0.0),
        }

    def generate_data(self) -> pd.DataFrame:
        """
        Generate the complete dataset.

        Returns:
            Pandas DataFrame with generated taxi trip data
        """
        logger.info(f"Generating {self.num_records} taxi trip records...")

        trips = []
        for i in range(self.num_records):
            trip = self._generate_trip(i)
            trips.append(trip)

            if (i + 1) % 1000 == 0:
                logger.info(f"Generated {i + 1} / {self.num_records} records")

        df = pd.DataFrame(trips)

        # Sort by pickup time
        df = df.sort_values('tpep_pickup_datetime').reset_index(drop=True)

        logger.info(f"Successfully generated {len(df)} records")
        logger.info(f"Date range: {df['tpep_pickup_datetime'].min()} to {df['tpep_pickup_datetime'].max()}")
        logger.info(f"Total revenue: ${df['total_amount'].sum():,.2f}")
        logger.info(f"Average trip distance: {df['trip_distance'].mean():.2f} miles")
        logger.info(f"Average fare: ${df['fare_amount'].mean():.2f}")

        return df

    def save_parquet(self, df: pd.DataFrame, output_path: str) -> None:
        """
        Save the DataFrame as a Parquet file.

        Args:
            df: DataFrame to save
            output_path: Path to save the Parquet file
        """
        logger.info(f"Saving data to {output_path}")

        # Convert DataFrame to PyArrow Table for better control
        table = pa.Table.from_pandas(df)

        # Write to Parquet with compression
        pq.write_table(
            table,
            output_path,
            compression='snappy',
            use_dictionary=True,
            write_statistics=True
        )

        file_size = Path(output_path).stat().st_size / (1024 * 1024)
        logger.info(f"Successfully saved {len(df)} records to {output_path} ({file_size:.2f} MB)")


def main():
    """Main entry point for the demo data generator."""
    parser = argparse.ArgumentParser(
        description='Generate synthetic NYC taxi trip data for demo purposes'
    )
    parser.add_argument(
        '--num-records',
        type=int,
        default=10000,
        help='Number of records to generate (default: 10000)'
    )
    parser.add_argument(
        '--year',
        type=int,
        default=2024,
        help='Year for the data (default: 2024)'
    )
    parser.add_argument(
        '--month',
        type=int,
        default=3,
        help='Month for the data (default: 3 for March)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='yellow_tripdata_2024-03.parquet',
        help='Output file path (default: yellow_tripdata_2024-03.parquet)'
    )

    args = parser.parse_args()

    # Validate month
    if not 1 <= args.month <= 12:
        logger.error(f"Invalid month: {args.month}. Must be between 1 and 12.")
        sys.exit(1)

    try:
        # Generate data
        generator = TaxiDataGenerator(
            num_records=args.num_records,
            year=args.year,
            month=args.month
        )

        df = generator.generate_data()

        # Save to Parquet
        generator.save_parquet(df, args.output)

        logger.info("Demo data generation completed successfully!")

    except Exception as e:
        logger.error(f"Error generating demo data: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
