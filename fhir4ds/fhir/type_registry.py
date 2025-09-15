"""
FHIR Type Registry System

Provides configuration-driven handling of FHIR complex types, eliminating
hardcoded field names and enabling flexible SQL generation across dialects.

This replaces hardcoded paths like $.family, $.given with configurable
type-aware processing that follows CLAUDE.md "No Hardcoded Values" principle.
"""

import json
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Comprehensive FHIR R4 Complex Types Registry
# Based on official FHIR R4 specification from hl7.org/fhir/R4/datatypes.html
FHIR_COMPLEX_TYPES = {
    "HumanName": {
        "fields": ["use", "text", "family", "given", "prefix", "suffix", "period"],
        "arrays": ["given", "prefix", "suffix"],
        "required": [],
        "display_template": "{family}, {given.join(' ')}"
    },
    "Address": {
        "fields": ["use", "type", "text", "line", "city", "district", "state", "postalCode", "country", "period"],
        "arrays": ["line"],
        "required": [],
        "display_template": "{line.join(', ')}, {city}, {state} {postalCode}"
    },
    "ContactPoint": {
        "fields": ["system", "value", "use", "rank", "period"],
        "arrays": [],
        "required": ["system", "value"],
        "display_template": "{value} ({system})"
    },
    "CodeableConcept": {
        "fields": ["coding", "text"],
        "arrays": ["coding"],
        "required": [],
        "display_template": "{coding[0].display || text}"
    },
    "Coding": {
        "fields": ["system", "version", "code", "display", "userSelected"],
        "arrays": [],
        "required": ["system", "code"],
        "display_template": "{display} ({code})"
    },
    "Identifier": {
        "fields": ["use", "type", "system", "value", "period", "assigner"],
        "arrays": [],
        "required": ["value"],
        "display_template": "{value} ({system})"
    },
    "Period": {
        "fields": ["start", "end"],
        "arrays": [],
        "required": [],
        "display_template": "{start} to {end}"
    },
    "Quantity": {
        "fields": ["value", "comparator", "unit", "system", "code"],
        "arrays": [],
        "required": ["value"],
        "display_template": "{value} {unit}"
    },
    "Range": {
        "fields": ["low", "high"],
        "arrays": [],
        "required": [],
        "display_template": "{low.value}-{high.value} {low.unit}"
    },
    "Ratio": {
        "fields": ["numerator", "denominator"],
        "arrays": [],
        "required": [],
        "display_template": "{numerator.value}:{denominator.value}"
    },
    "Attachment": {
        "fields": ["contentType", "language", "data", "url", "size", "hash", "title", "creation"],
        "arrays": [],
        "required": [],
        "display_template": "{title} ({contentType})"
    },
    "Annotation": {
        "fields": ["author", "time", "text"],
        "arrays": [],
        "required": ["text"],
        "display_template": "{text} ({author})"
    },
    "Timing": {
        "fields": ["event", "repeat", "code"],
        "arrays": ["event"],
        "required": [],
        "display_template": "{code.text || repeat.frequency + ' ' + repeat.period}"
    },
    "Signature": {
        "fields": ["type", "when", "who", "onBehalfOf", "targetFormat", "sigFormat", "data"],
        "arrays": ["type"],
        "required": ["type", "when", "who"],
        "display_template": "{type[0].display} by {who.display}"
    },
    "SampledData": {
        "fields": ["origin", "period", "factor", "lowerLimit", "upperLimit", "dimensions", "data"],
        "arrays": [],
        "required": ["origin", "period", "dimensions"],
        "display_template": "{dimensions}D sampled data"
    },
    "Reference": {
        "fields": ["reference", "type", "identifier", "display"],
        "arrays": [],
        "required": [],
        "display_template": "{display || reference}"
    },
    "Meta": {
        "fields": ["versionId", "lastUpdated", "source", "profile", "security", "tag"],
        "arrays": ["profile", "security", "tag"],
        "required": [],
        "display_template": "v{versionId} updated {lastUpdated}"
    }
}


@dataclass
class FHIRTypeStructure:
    """Represents the structure of a FHIR complex type."""
    fields: List[str]
    arrays: List[str]
    required: List[str]
    display_template: str
    
    def is_array_field(self, field_name: str) -> bool:
        """Check if a field is an array type."""
        return field_name in self.arrays
    
    def is_required_field(self, field_name: str) -> bool:
        """Check if a field is required."""
        return field_name in self.required


class FHIRTypeRegistry:
    """
    Registry for FHIR complex types providing configuration-driven field handling.
    
    This eliminates hardcoded FHIR field names from dialect implementations
    and enables flexible, type-aware SQL generation.
    """
    
    def __init__(self):
        """Initialize the registry with FHIR R4 complex types."""
        self.types = FHIR_COMPLEX_TYPES
        logger.debug(f"Initialized FHIR Type Registry with {len(self.types)} complex types")
    
    def get_type_structure(self, type_name: str) -> Optional[Dict[str, Any]]:
        """
        Get the structure definition for a FHIR type.
        
        Args:
            type_name: FHIR type name (e.g., 'HumanName', 'Address')
            
        Returns:
            Type structure dictionary or None if not found
        """
        return self.types.get(type_name)
    
    def get_display_fields(self, type_name: str) -> List[str]:
        """
        Get all field names for a FHIR type.
        
        Args:
            type_name: FHIR type name
            
        Returns:
            List of field names for the type
        """
        type_info = self.get_type_structure(type_name)
        return type_info["fields"] if type_info else []
    
    def is_array_field(self, type_name: str, field_name: str) -> bool:
        """
        Check if a field is an array type.
        
        Args:
            type_name: FHIR type name
            field_name: Field name to check
            
        Returns:
            True if field is an array type
        """
        type_info = self.get_type_structure(type_name)
        return field_name in type_info.get("arrays", []) if type_info else False
    
    def is_required_field(self, type_name: str, field_name: str) -> bool:
        """
        Check if a field is required.
        
        Args:
            type_name: FHIR type name  
            field_name: Field name to check
            
        Returns:
            True if field is required
        """
        type_info = self.get_type_structure(type_name)
        return field_name in type_info.get("required", []) if type_info else False
    
    def get_display_template(self, type_name: str) -> str:
        """
        Get display template for a FHIR type.
        
        Args:
            type_name: FHIR type name
            
        Returns:
            Display template string
        """
        type_info = self.get_type_structure(type_name)
        return type_info.get("display_template", "{text || value}") if type_info else "{text || value}"
    
    def get_supported_types(self) -> List[str]:
        """
        Get list of all supported FHIR complex types.
        
        Returns:
            List of supported type names
        """
        return list(self.types.keys())
    
    def is_supported_type(self, type_name: str) -> bool:
        """
        Check if a FHIR type is supported by this registry.
        
        Args:
            type_name: FHIR type name to check
            
        Returns:
            True if type is supported
        """
        return type_name in self.types


# Global registry instance for convenient access
_global_registry = FHIRTypeRegistry()

def get_fhir_type_registry() -> FHIRTypeRegistry:
    """Get the global FHIR type registry instance."""
    return _global_registry