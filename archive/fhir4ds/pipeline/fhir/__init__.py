"""
FHIR support for unified pipeline architecture.

This module provides FHIR-specific types and registry functionality
consolidated into the unified pipeline structure.
"""

from .type_registry import FHIRTypeRegistry

__all__ = ['FHIRTypeRegistry']