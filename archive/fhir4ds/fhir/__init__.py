"""
FHIR Support Module

This module provides FHIR-specific functionality including type registries,
structure definitions, and configuration-driven processing.
"""

from .type_registry import FHIRTypeRegistry, FHIR_COMPLEX_TYPES

__all__ = ['FHIRTypeRegistry', 'FHIR_COMPLEX_TYPES']