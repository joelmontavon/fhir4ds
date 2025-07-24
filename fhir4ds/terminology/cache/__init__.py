"""
Terminology Caching Module

Provides multi-tier caching for terminology service operations including
in-memory dict cache and DuckDB persistent cache.
"""

from .cache_manager import TerminologyCache

__all__ = ['TerminologyCache']