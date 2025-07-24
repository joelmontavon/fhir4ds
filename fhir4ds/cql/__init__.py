"""
Clinical Quality Language (CQL) module for FHIR4DS.

This module provides CQL support as a layered extension on top of the existing
FHIRPath implementation. It reuses all existing FHIRPath functionality while
adding CQL-specific features like library management, clinical contexts,
and quality measure logic.
"""

__version__ = "0.3.0"
__author__ = "FHIR4DS Development Team"

# Import main CQL components
from .core.engine import CQLEngine
from .core.context import CQLContext, CQLContextType, CQLContextManager
from .measures.population import QualityMeasureDefinition, PopulationCriteria, PopulationType, QualityMeasureBuilder
from .measures.scoring import MeasureScoring, ScoringMethod
from .measures.quality import QualityMeasureEngine, QualityMeasureRegistry, quality_measure_registry, initialize_default_measures
from .functions.clinical import ClinicalFunctions, TerminologyFunctions, ClinicalLogicFunctions

# Initialize default measures
initialize_default_measures()

# Main exports
__all__ = [
    'CQLEngine',
    'CQLContext', 
    'CQLContextType',
    'CQLContextManager',
    'QualityMeasureDefinition',
    'PopulationCriteria', 
    'PopulationType',
    'QualityMeasureBuilder',
    'MeasureScoring',
    'ScoringMethod', 
    'QualityMeasureEngine',
    'QualityMeasureRegistry',
    'quality_measure_registry',
    'ClinicalFunctions',
    'TerminologyFunctions',
    'ClinicalLogicFunctions'
]