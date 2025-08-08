"""
CQL Navigation Module - Code Validation and Navigation Features

This module provides advanced navigation and validation capabilities for CQL implementations:
- Individual code validation against authoritative terminology sources  
- Code appropriateness scoring for clinical contexts
- Integration with VSAC, FHIR terminology servers, and other sources
"""

from .code_validator import (
    CodeValidator,
    VSACValidator, 
    FHIRTerminologyValidator,
    ValidationResult,
    CodeValidationResult
)

from .code_appropriateness import (
    ClinicalContext,
    SpecificityLevel,
    AppropriatenessScore,
    CodeAppropriatenessScorer,
    DefaultAppropriatenessScorer,
    create_appropriateness_scorer,
    score_code_appropriateness
)

__all__ = [
    # Code validation
    'CodeValidator',
    'VSACValidator', 
    'FHIRTerminologyValidator',
    'ValidationResult',
    'CodeValidationResult',
    # Code appropriateness scoring
    'ClinicalContext',
    'SpecificityLevel', 
    'AppropriatenessScore',
    'CodeAppropriatenessScorer',
    'DefaultAppropriatenessScorer',
    'create_appropriateness_scorer',
    'score_code_appropriateness'
]