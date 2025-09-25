"""
CQL Context Provider Pattern

This module implements the Context Provider Pattern for smart context detection and injection
in CQL function calls. It automatically provides patient context, encounter context, and other
clinical contexts where needed by CQL functions.

Key Features:
- Smart context detection based on function signatures
- Automatic patient context injection for age calculations
- Resource context resolution for [Patient], [Condition], etc. queries
- Context stack management for nested scopes
- Compatible with existing function registry architecture
"""

import logging
from typing import Dict, Any, Optional, List, Union, Set
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class ContextType(Enum):
    """Types of clinical contexts available in CQL evaluation."""
    PATIENT = "Patient"
    ENCOUNTER = "Encounter"
    CONDITION = "Condition"
    MEDICATION = "Medication"
    OBSERVATION = "Observation"
    PROCEDURE = "Procedure"
    POPULATION = "Population"


@dataclass
class ContextMetadata:
    """
    Metadata about available context for a CQL evaluation session.
    """
    patient_data: Optional[Dict[str, Any]] = None
    encounter_data: Optional[Dict[str, Any]] = None
    available_resources: Set[str] = field(default_factory=set)
    context_parameters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ContextInjectionRule:
    """
    Rule for determining when and how to inject context into function calls.
    """
    function_pattern: str  # Function name pattern (exact match or regex)
    required_context: ContextType
    auto_inject_fields: Dict[str, str] = field(default_factory=dict)  # parameter -> context_path
    conditional_injection: Optional[str] = None  # Python expression for conditional injection
    

class CQLContextProvider:
    """
    Context Provider for CQL functions with smart context detection and injection.
    
    This provider automatically detects when CQL functions need clinical context
    and injects the appropriate data without requiring explicit context parameters
    in the CQL expressions.
    """
    
    def __init__(self, current_context: Optional[str] = None):
        """
        Initialize the context provider.
        
        Args:
            current_context: Initial context (Patient, Population, etc.)
        """
        self.current_context = current_context or "Patient"
        self.context_stack = []  # For nested context scopes
        self.context_metadata = ContextMetadata()
        
        # Initialize context injection rules
        self.injection_rules = self._initialize_injection_rules()
        
        logger.info(f"CQL Context Provider initialized with context: {self.current_context}")
    
    def _initialize_injection_rules(self) -> List[ContextInjectionRule]:
        """Initialize the built-in context injection rules."""
        return [
            # Age calculation functions need patient birth date
            ContextInjectionRule(
                function_pattern="AgeInYears",
                required_context=ContextType.PATIENT,
                auto_inject_fields={"birth_date": "birthDate"}
            ),
            ContextInjectionRule(
                function_pattern="AgeInMonths", 
                required_context=ContextType.PATIENT,
                auto_inject_fields={"birth_date": "birthDate"}
            ),
            ContextInjectionRule(
                function_pattern="AgeInDays",
                required_context=ContextType.PATIENT,
                auto_inject_fields={"birth_date": "birthDate"}
            ),
            ContextInjectionRule(
                function_pattern="AgeInYearsAt",
                required_context=ContextType.PATIENT,
                auto_inject_fields={"birth_date": "birthDate"}
            ),
            
            # Resource existence functions need appropriate context
            ContextInjectionRule(
                function_pattern="exists",
                required_context=ContextType.PATIENT,
                conditional_injection="contains_resource_query(args)"
            ),
            
            # Date/time functions that might need patient context
            ContextInjectionRule(
                function_pattern="CalculateAge",
                required_context=ContextType.PATIENT,
                auto_inject_fields={"birth_date": "birthDate"}
            )
        ]
    
    def detect_context_requirement(self, function_name: str, args: List[Any]) -> Optional[ContextType]:
        """
        Detect if a function call requires context injection.
        
        Args:
            function_name: Name of the function being called
            args: Function arguments
            
        Returns:
            Required context type, or None if no context needed
        """
        # Check against injection rules
        for rule in self.injection_rules:
            if self._matches_function_pattern(function_name, rule.function_pattern):
                # Check conditional injection if specified
                if rule.conditional_injection:
                    if self._evaluate_conditional_injection(rule.conditional_injection, function_name, args):
                        logger.debug(f"Context required for {function_name}: {rule.required_context}")
                        return rule.required_context
                else:
                    logger.debug(f"Context required for {function_name}: {rule.required_context}")
                    return rule.required_context
        
        # Check if function name suggests resource context
        resource_indicators = ['Patient', 'Condition', 'Encounter', 'Medication', 'Observation']
        if any(indicator in function_name for indicator in resource_indicators):
            logger.debug(f"Resource context detected for {function_name}")
            return ContextType.PATIENT
        
        return None
    
    def inject_patient_context(self, function_call: Dict[str, Any]) -> Dict[str, Any]:
        """
        Inject patient context into a function call.
        
        Args:
            function_call: Original function call parameters
            
        Returns:
            Enhanced function call with patient context
        """
        enhanced_call = function_call.copy()
        function_name = enhanced_call.get('function_name', '')
        
        if not self.context_metadata.patient_data:
            logger.warning("Patient context injection requested but no patient data available")
            # Still add None patient_context for consistency
            enhanced_call['patient_context'] = None
            return enhanced_call
        
        # Find applicable injection rule
        for rule in self.injection_rules:
            if self._matches_function_pattern(function_name, rule.function_pattern):
                for param_name, context_path in rule.auto_inject_fields.items():
                    if param_name not in enhanced_call:
                        # Extract value from patient context
                        injected_value = self._extract_from_context(
                            self.context_metadata.patient_data, 
                            context_path
                        )
                        if injected_value:
                            enhanced_call[param_name] = injected_value
                            logger.debug(f"Injected {param_name}={injected_value} for {function_name}")
                        else:
                            logger.warning(f"Could not extract {context_path} from patient context")
                break
        
        # Always add patient_context for functions that might need it
        if 'patient_context' not in enhanced_call:
            enhanced_call['patient_context'] = self.context_metadata.patient_data
        
        return enhanced_call
    
    def resolve_resource_context(self, resource_type: str, filters: Optional[Dict[str, Any]] = None) -> str:
        """
        Resolve resource queries like [Patient], [Condition] to SQL with appropriate context.
        
        Args:
            resource_type: FHIR resource type (Patient, Condition, etc.)
            filters: Optional filters for the resource query
            
        Returns:
            SQL query with patient context applied
        """
        base_query = f"SELECT * FROM {resource_type.lower()}"
        where_clauses = []
        
        # Add patient context filtering for non-Patient resources
        if resource_type != "Patient" and self.current_context == "Patient":
            where_clauses.append("patient_id = {patient_id}")
        
        # Add filters if provided
        if filters:
            for filter_key, filter_value in filters.items():
                if isinstance(filter_value, str):
                    where_clauses.append(f"{filter_key} = '{filter_value}'")
                else:
                    where_clauses.append(f"{filter_key} = {filter_value}")
        
        # Construct final query
        if where_clauses:
            base_query += " WHERE " + " AND ".join(where_clauses)
        
        logger.debug(f"Resolved resource query for {resource_type}: {base_query}")
        return base_query
    
    def set_patient_context(self, patient_data: Dict[str, Any]):
        """
        Set the current patient context data.
        
        Args:
            patient_data: Patient FHIR resource or patient data dictionary
        """
        self.context_metadata.patient_data = patient_data
        
        # Extract available resource types from patient data
        if 'resourceType' in patient_data:
            self.context_metadata.available_resources.add(patient_data['resourceType'])
        
        logger.info(f"Patient context set: patient_id={patient_data.get('id', 'unknown')}")
    
    def push_context(self, new_context: str):
        """
        Push a new context onto the context stack (for nested scopes).
        
        Args:
            new_context: New context to push
        """
        self.context_stack.append(self.current_context)
        self.current_context = new_context
        logger.debug(f"Context pushed: {new_context} (stack depth: {len(self.context_stack)})")
    
    def pop_context(self) -> Optional[str]:
        """
        Pop the previous context from the context stack.
        
        Returns:
            Previous context, or None if stack is empty
        """
        if self.context_stack:
            previous_context = self.current_context
            self.current_context = self.context_stack.pop()
            logger.debug(f"Context popped: {self.current_context} (was {previous_context})")
            return previous_context
        else:
            logger.warning("Cannot pop context: stack is empty")
            return None
    
    def _matches_function_pattern(self, function_name: str, pattern: str) -> bool:
        """Check if function name matches the pattern (exact match for now)."""
        return function_name.lower() == pattern.lower()
    
    def _evaluate_conditional_injection(self, condition: str, function_name: str, args: List[Any]) -> bool:
        """
        Evaluate conditional injection expression.
        
        Args:
            condition: Python expression to evaluate
            function_name: Current function name
            args: Function arguments
            
        Returns:
            True if context should be injected
        """
        # For now, implement specific conditions
        if condition == "contains_resource_query(args)":
            # Check if any argument contains resource query syntax
            return any(
                isinstance(arg, str) and ('[' in arg and ']' in arg) 
                for arg in args
            )
        
        return False
    
    def _extract_from_context(self, context_data: Dict[str, Any], path: str) -> Any:
        """
        Extract value from context data using dot notation path.
        
        Args:
            context_data: Context data dictionary
            path: Dot notation path (e.g., 'birthDate', 'name.family')
            
        Returns:
            Extracted value or None
        """
        try:
            current = context_data
            for part in path.split('.'):
                if isinstance(current, dict):
                    current = current.get(part)
                elif isinstance(current, list) and part.isdigit():
                    index = int(part)
                    current = current[index] if 0 <= index < len(current) else None
                else:
                    return None
                
                if current is None:
                    return None
            
            return current
        except (KeyError, IndexError, ValueError):
            return None
    
    def get_context_summary(self) -> Dict[str, Any]:
        """Get summary of current context state for debugging."""
        return {
            'current_context': self.current_context,
            'context_stack_depth': len(self.context_stack),
            'has_patient_data': self.context_metadata.patient_data is not None,
            'available_resources': list(self.context_metadata.available_resources),
            'injection_rules_count': len(self.injection_rules)
        }