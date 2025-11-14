"""
NYC Taxi Data Processing Module

This module contains scripts for downloading, exploring, cleaning, and sampling
NYC Yellow Taxi trip data for analysis and RDS import.
"""

__version__ = '1.0.0'
__author__ = 'DE Intern 2024'

from pathlib import Path

# Default data directories
DATA_DIR = Path(__file__).parent.parent.parent / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'

# Ensure directories exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
