"""
Database module for NYC Taxi Data Analytics.

This module provides utilities for loading and managing NYC taxi data
in RDS PostgreSQL with a star schema design.
"""

from pathlib import Path

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

# Module exports
__all__ = [
    'RDSDataLoader',
]

# Lazy imports to avoid circular dependencies
def _get_loader():
    from .load_rds_data import RDSDataLoader
    return RDSDataLoader

# Make available at module level
RDSDataLoader = property(lambda self: _get_loader())
