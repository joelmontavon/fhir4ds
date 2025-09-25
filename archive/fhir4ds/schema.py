"""
FHIR Schema Management for FHIR4DS

This module provides centralized FHIR resource schema information to replace
hardcoded array field lists throughout the codebase. This follows CLAUDE.md
principles of addressing root causes and avoiding band-aid fixes.
"""

from typing import Dict, Set, List, Optional
import logging

logger = logging.getLogger(__name__)


class FHIRSchemaManager:
    """
    Centralized FHIR schema management for array field detection.
    
    This replaces hardcoded lists scattered throughout the codebase with
    a single, authoritative source of FHIR schema information.
    """
    
    # FHIR R4 Resource Array Fields
    # Based on FHIR R4 specification: https://hl7.org/fhir/R4/
    FHIR_ARRAY_FIELDS = {
        # Core resource fields that are arrays across multiple resource types
        'identifier': {
            'description': 'Business identifiers assigned to this resource',
            'resources': ['Patient', 'Practitioner', 'Organization', 'Location', 'Device', 'etc.']
        },
        'telecom': {
            'description': 'Contact details (phone, email, etc.)',
            'resources': ['Patient', 'Practitioner', 'Organization', 'Location', 'etc.']
        },
        'address': {
            'description': 'Physical addresses',
            'resources': ['Patient', 'Practitioner', 'Organization', 'Location', 'etc.']
        },
        'name': {
            'description': 'Human names (for Patient, Practitioner, etc.)',
            'resources': ['Patient', 'Practitioner', 'Person', 'RelatedPerson']
        },
        'communication': {
            'description': 'Languages of communication',
            'resources': ['Patient', 'Practitioner', 'RelatedPerson']
        },
        'contact': {
            'description': 'Contact parties (guardians, emergency contacts, etc.)',
            'resources': ['Patient', 'Organization']
        },
        
        # Clinical resource array fields
        'category': {
            'description': 'Classification/categorization',
            'resources': ['Observation', 'DiagnosticReport', 'Condition', 'etc.']
        },
        'code': {
            'description': 'Coded values (can be array in some contexts)',
            'resources': ['Observation', 'DiagnosticReport', 'Procedure', 'etc.']
        },
        'coding': {
            'description': 'Code defined by terminology system',
            'resources': ['CodeableConcept', 'Coding']
        },
        'component': {
            'description': 'Component observations (e.g., BP systolic/diastolic)',
            'resources': ['Observation']
        },
        'performer': {
            'description': 'Who performed the observation/procedure',
            'resources': ['Observation', 'DiagnosticReport', 'Procedure', 'etc.']
        },
        'interpretation': {
            'description': 'High/low/normal flags',
            'resources': ['Observation', 'DiagnosticReport']
        },
        'referenceRange': {
            'description': 'Reference ranges for observations',
            'resources': ['Observation']
        },
        'related': {
            'description': 'Related observations/reports',
            'resources': ['Observation', 'DiagnosticReport']
        },
        'bodySite': {
            'description': 'Anatomical location',
            'resources': ['Observation', 'Procedure', 'Condition']
        },
        
        # Medication-related array fields
        'dosage': {
            'description': 'Dosage instructions',
            'resources': ['MedicationRequest', 'MedicationAdministration']
        },
        'ingredient': {
            'description': 'Medication ingredients',
            'resources': ['Medication']
        },
        
        # Diagnostic/procedure array fields
        'specimen': {
            'description': 'Specimens used',
            'resources': ['DiagnosticReport', 'Observation']
        },
        'result': {
            'description': 'Test results',
            'resources': ['DiagnosticReport']
        },
        'image': {
            'description': 'Associated images',
            'resources': ['DiagnosticReport', 'Observation']
        },
        'media': {
            'description': 'Associated media',
            'resources': ['DiagnosticReport', 'Observation']
        },
        
        # Care/encounter array fields
        'participant': {
            'description': 'Care team participants',
            'resources': ['CareTeam', 'Encounter']
        },
        'diagnosis': {
            'description': 'Encounter diagnoses',
            'resources': ['Encounter']
        },
        'reason': {
            'description': 'Reasons for care/procedures',
            'resources': ['Encounter', 'Procedure', 'MedicationRequest']
        },
        'location': {
            'description': 'Service locations',
            'resources': ['Encounter']
        },
        
        # Extensions and metadata
        'extension': {
            'description': 'Additional metadata/extensions',
            'resources': ['All FHIR resources']
        },
        'modifierExtension': {
            'description': 'Extensions that modify meaning',
            'resources': ['All FHIR resources']
        },
        'contained': {
            'description': 'Contained resources',
            'resources': ['All FHIR resources']
        }
    }
    
    # Common scalar fields for optimization (fields that are typically NOT arrays)
    FHIR_SCALAR_FIELDS = {
        'id', 'resourceType', 'meta', 'implicitRules', 'language',
        'text', 'status', 'active', 'gender', 'birthDate', 'deceased',
        'multipleBirth', 'managingOrganization', 'subject', 'encounter',
        'effective', 'issued', 'value', 'dataAbsentReason', 'method',
        'device', 'specimen', 'focus', 'basedOn', 'partOf', 'intent',
        'priority', 'doNotPerform', 'authoredOn', 'requester', 'recorder',
        'asserter', 'lastOccurrence', 'note'
    }
    
    def __init__(self):
        """Initialize the FHIR schema manager"""
        self._array_fields_cache = set(self.FHIR_ARRAY_FIELDS.keys())
        logger.info(f"Initialized FHIR schema manager with {len(self._array_fields_cache)} array fields")
    
    def is_array_field(self, field_name: str) -> bool:
        """
        Check if a FHIR field is typically an array.
        
        Args:
            field_name: The field name to check (e.g., 'name', 'telecom')
            
        Returns:
            True if the field is typically an array in FHIR resources
        """
        return field_name.lower() in self._array_fields_cache
    
    def is_scalar_field(self, field_name: str) -> bool:
        """
        Check if a FHIR field is typically a scalar (single value).
        
        Args:
            field_name: The field name to check
            
        Returns:
            True if the field is typically a scalar in FHIR resources
        """
        return field_name.lower() in self.FHIR_SCALAR_FIELDS
    
    def get_array_fields(self) -> Set[str]:
        """Get all known FHIR array fields"""
        return self._array_fields_cache.copy()
    
    def get_legacy_array_fields_list(self) -> List[str]:
        """
        Get array fields as a list for backward compatibility.
        
        This method supports migration from hardcoded lists while
        the codebase transitions to use is_array_field() directly.
        """
        return sorted(list(self._array_fields_cache))
    
    def get_field_info(self, field_name: str) -> Optional[Dict]:
        """
        Get detailed information about a FHIR field.
        
        Args:
            field_name: The field name to look up
            
        Returns:
            Dictionary with field information or None if not found
        """
        return self.FHIR_ARRAY_FIELDS.get(field_name.lower())
    
    def add_custom_array_field(self, field_name: str, description: str = None, resources: List[str] = None):
        """
        Add a custom array field for organization-specific FHIR extensions.
        
        Args:
            field_name: Name of the custom field
            description: Description of the field
            resources: List of resources that use this field
        """
        field_info = {
            'description': description or f'Custom array field: {field_name}',
            'resources': resources or ['Custom']
        }
        
        self.FHIR_ARRAY_FIELDS[field_name.lower()] = field_info
        self._array_fields_cache.add(field_name.lower())
        
        logger.info(f"Added custom array field: {field_name}")


# Global instance for easy access throughout the codebase
fhir_schema = FHIRSchemaManager()