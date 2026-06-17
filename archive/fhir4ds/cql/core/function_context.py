"""
Function Context Metadata

This module defines the data structures for representing function context requirements
and metadata used by the Context Provider Pattern.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from enum import Enum


class FunctionContextType(Enum):
    """Types of context that functions may require."""
    PATIENT = "patient"
    ENCOUNTER = "encounter" 
    CONDITION = "condition"
    MEDICATION = "medication"
    OBSERVATION = "observation"
    PROCEDURE = "procedure"
    POPULATION = "population"
    NONE = "none"


@dataclass
class FunctionContext:
    """
    Metadata describing the context requirements of a CQL function.
    
    This class captures what clinical context a function needs for proper execution,
    including automatic parameter injection and context-dependent behavior.
    """
    
    # Context requirements
    requires_patient_context: bool = False
    requires_encounter_context: bool = False
    requires_condition_context: bool = False
    requires_medication_context: bool = False
    
    # Context parameters that should be auto-injected
    context_parameters: List[str] = field(default_factory=list)
    
    # Mapping of parameter names to context field paths for auto-injection
    # e.g., {"birth_date": "Patient.birthDate", "gender": "Patient.gender"}
    auto_inject_fields: Dict[str, str] = field(default_factory=dict)
    
    # Whether this function can operate without context (fallback behavior)
    context_optional: bool = True
    
    # Primary context type for this function
    primary_context: FunctionContextType = FunctionContextType.NONE
    
    # Additional context types this function can utilize
    secondary_contexts: List[FunctionContextType] = field(default_factory=list)
    
    # Whether function results depend on patient-specific data
    patient_dependent: bool = False
    
    # Whether function can be evaluated at population level
    population_evaluable: bool = True
    
    def requires_any_context(self) -> bool:
        """Check if function requires any clinical context."""
        return (
            self.requires_patient_context or
            self.requires_encounter_context or
            self.requires_condition_context or
            self.requires_medication_context or
            bool(self.context_parameters) or
            bool(self.auto_inject_fields)
        )
    
    def get_required_context_types(self) -> List[FunctionContextType]:
        """Get list of all required context types."""
        required = []
        
        if self.requires_patient_context:
            required.append(FunctionContextType.PATIENT)
        if self.requires_encounter_context:
            required.append(FunctionContextType.ENCOUNTER)
        if self.requires_condition_context:
            required.append(FunctionContextType.CONDITION)
        if self.requires_medication_context:
            required.append(FunctionContextType.MEDICATION)
            
        return required
    
    def can_auto_inject(self, parameter_name: str) -> bool:
        """Check if a parameter can be auto-injected from context."""
        return parameter_name in self.auto_inject_fields
    
    def get_injection_path(self, parameter_name: str) -> Optional[str]:
        """Get the context path for auto-injecting a parameter."""
        return self.auto_inject_fields.get(parameter_name)


# Pre-defined context metadata for common CQL functions
STANDARD_FUNCTION_CONTEXTS = {
    
    # Age calculation functions - require patient birth date
    "AgeInYears": FunctionContext(
        requires_patient_context=True,
        primary_context=FunctionContextType.PATIENT,
        auto_inject_fields={"birth_date": "birthDate"},
        patient_dependent=True,
        context_optional=False
    ),
    
    "AgeInMonths": FunctionContext(
        requires_patient_context=True,
        primary_context=FunctionContextType.PATIENT,
        auto_inject_fields={"birth_date": "birthDate"},
        patient_dependent=True,
        context_optional=False
    ),
    
    "AgeInDays": FunctionContext(
        requires_patient_context=True,
        primary_context=FunctionContextType.PATIENT,
        auto_inject_fields={"birth_date": "birthDate"},
        patient_dependent=True,
        context_optional=False
    ),
    
    "AgeInYearsAt": FunctionContext(
        requires_patient_context=True,
        primary_context=FunctionContextType.PATIENT,
        auto_inject_fields={"birth_date": "birthDate"},
        patient_dependent=True,
        context_optional=False
    ),
    
    "CalculateAge": FunctionContext(
        requires_patient_context=True,
        primary_context=FunctionContextType.PATIENT,
        auto_inject_fields={"birth_date": "birthDate"},
        patient_dependent=True,
        context_optional=False
    ),
    
    # Existence and resource query functions - may need resource context
    "exists": FunctionContext(
        primary_context=FunctionContextType.PATIENT,
        secondary_contexts=[
            FunctionContextType.CONDITION,
            FunctionContextType.ENCOUNTER,
            FunctionContextType.MEDICATION,
            FunctionContextType.OBSERVATION
        ],
        patient_dependent=True,
        population_evaluable=True
    ),
    
    "empty": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
    
    "count": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
    
    # Mathematical functions - generally context-independent
    "Sum": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
    
    "Min": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
    
    "Max": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
    
    "Avg": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
    
    "StdDev": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
    
    "Variance": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
    
    # Date/time functions - may or may not need context
    "Today": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
    
    "Now": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
    
    "DateTime": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
    
    # String functions - context-independent
    "Combine": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
    
    "Split": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
    
    "Length": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
    
    # Logical functions - context-independent
    "And": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
    
    "Or": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
    
    "Not": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
    
    # Nullological functions - context-independent
    "Coalesce": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
    
    "IsNull": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
    
    "IsTrue": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
    
    "IsFalse": FunctionContext(
        population_evaluable=True,
        patient_dependent=False
    ),
}


def get_function_context(function_name: str) -> FunctionContext:
    """
    Get the context metadata for a function.
    
    Args:
        function_name: Name of the CQL function
        
    Returns:
        FunctionContext metadata for the function
    """
    # Check exact match first
    if function_name in STANDARD_FUNCTION_CONTEXTS:
        return STANDARD_FUNCTION_CONTEXTS[function_name]
    
    # Check case-insensitive match
    for name, context in STANDARD_FUNCTION_CONTEXTS.items():
        if name.lower() == function_name.lower():
            return context
    
    # Return default context for unknown functions
    return FunctionContext(
        primary_context=FunctionContextType.NONE,
        population_evaluable=True,
        patient_dependent=False,
        context_optional=True
    )


def register_function_context(function_name: str, context: FunctionContext):
    """
    Register context metadata for a custom function.
    
    Args:
        function_name: Name of the function
        context: Context metadata
    """
    STANDARD_FUNCTION_CONTEXTS[function_name] = context