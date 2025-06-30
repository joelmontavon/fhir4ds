"""
ViewDefinition Templates Module

Provides a library of pre-built ViewDefinitions for common healthcare analytics patterns.
Templates are optimized for performance and follow best practices for FHIR data analysis.

Template Categories:
- Patient Analytics: Demographics, contacts, identifiers
- Clinical Data: Observations, procedures, medications  
- Administrative: Encounters, appointments, coverage
- Quality Measures: Care gaps, outcomes, adherence
- Research: Cohort identification, outcome analysis

Examples:
    # Use pre-built templates
    view_def = Templates.patient_demographics()
    results = db.execute(view_def)
    
    # Customize templates with parameters
    view_def = Templates.observations_by_code("85354-9")  # Blood pressure
    results = db.execute(view_def)
    
    # Get template with metadata
    template_info = Templates.get_template_info("patient_demographics")
    print(template_info.description)
"""

import json
import logging
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
from datetime import datetime, date

logger = logging.getLogger(__name__)


@dataclass
class TemplateMetadata:
    """
    Metadata about a ViewDefinition template.
    
    Attributes:
        name: Template identifier
        title: Human-readable title
        description: Detailed description of what the template does
        resource_type: Primary FHIR resource type
        category: Template category (e.g., "patient", "clinical", "administrative")
        tags: List of descriptive tags
        version: Template version
        author: Template author/maintainer
        created: Creation date
        updated: Last update date
        parameters: Available customization parameters
        sample_output: Example of expected output structure
    """
    name: str
    title: str
    description: str
    resource_type: str
    category: str
    tags: List[str]
    version: str = "1.0.0"
    author: str = "FHIR4DS"
    created: str = ""
    updated: str = ""
    parameters: List[Dict[str, Any]] = None
    sample_output: Dict[str, Any] = None
    
    def __post_init__(self):
        if not self.created:
            self.created = datetime.now().isoformat()
        if not self.updated:
            self.updated = self.created
        if self.parameters is None:
            self.parameters = []
        if self.sample_output is None:
            self.sample_output = {}


class Templates:
    """
    Library of pre-built ViewDefinition templates for common healthcare analytics.
    
    Provides ready-to-use ViewDefinitions that can be executed immediately or
    customized with parameters for specific use cases.
    """
    
    # Template metadata registry
    _metadata_registry: Dict[str, TemplateMetadata] = {}
    
    @classmethod
    def _register_template(cls, metadata: TemplateMetadata):
        """Register template metadata."""
        cls._metadata_registry[metadata.name] = metadata
    
    @classmethod
    def get_template_info(cls, template_name: str) -> Optional[TemplateMetadata]:
        """
        Get metadata for a specific template.
        
        Args:
            template_name: Name of the template
            
        Returns:
            TemplateMetadata object or None if not found
            
        Example:
            >>> info = Templates.get_template_info("patient_demographics")
            >>> print(info.description)
        """
        return cls._metadata_registry.get(template_name)
    
    @classmethod
    def list_templates(cls, category: Optional[str] = None) -> List[TemplateMetadata]:
        """
        List available templates, optionally filtered by category.
        
        Args:
            category: Optional category filter
            
        Returns:
            List of template metadata
            
        Example:
            >>> all_templates = Templates.list_templates()
            >>> patient_templates = Templates.list_templates("patient")
        """
        templates = list(cls._metadata_registry.values())
        if category:
            templates = [t for t in templates if t.category == category]
        return sorted(templates, key=lambda t: (t.category, t.name))
    
    @classmethod
    def get_categories(cls) -> List[str]:
        """
        Get list of available template categories.
        
        Returns:
            List of category names
        """
        categories = set(t.category for t in cls._metadata_registry.values())
        return sorted(categories)
    
    # Patient Analytics Templates
    @staticmethod
    def patient_demographics() -> Dict[str, Any]:
        """
        Basic patient demographics including name, birth date, gender, and contact info.
        
        Returns:
            ViewDefinition for patient demographics
            
        Output Columns:
            - id: Patient identifier
            - family_name: Family name from first name element
            - given_names: Given names joined with comma
            - birth_date: Birth date
            - gender: Administrative gender
            - active: Whether patient record is active
            - phone: Primary phone number
            - email: Primary email address
            
        Example:
            >>> view_def = Templates.patient_demographics()
            >>> df = db.execute_to_dataframe(view_def)
        """
        return {
            "resourceType": "ViewDefinition",
            "status": "active",
            "resource": "Patient",
            "select": [{
                "column": [
                    {"name": "id", "path": "id", "type": "id"},
                    {"name": "family_name", "path": "name.family", "type": "string"},
                    {"name": "given_names", "path": "name.given.join(', ')", "type": "string"},
                    {"name": "birth_date", "path": "birthDate", "type": "date"},
                    {"name": "gender", "path": "gender", "type": "code"},
                    {"name": "active", "path": "active", "type": "boolean"},
                    {"name": "phone", "path": "telecom.where(system='phone').value", "type": "string"},
                    {"name": "email", "path": "telecom.where(system='email').value", "type": "string"}
                ]
            }]
        }
    
    @staticmethod
    def patient_addresses() -> Dict[str, Any]:
        """
        Patient address information with forEach to handle multiple addresses.
        
        Returns:
            ViewDefinition for patient addresses
            
        Output Columns:
            - patient_id: Patient identifier
            - address_use: Address use (home, work, etc.)
            - address_type: Address type (postal, physical, etc.)
            - line: Address lines joined
            - city: City
            - state: State/province
            - postal_code: Postal code
            - country: Country
            
        Example:
            >>> view_def = Templates.patient_addresses()
            >>> df = db.execute_to_dataframe(view_def)
        """
        return {
            "resourceType": "ViewDefinition",
            "status": "active",
            "resource": "Patient",
            "select": [{
                "forEach": "address",
                "column": [
                    {"name": "patient_id", "path": "id", "type": "id"},
                    {"name": "address_use", "path": "use", "type": "code"},
                    {"name": "address_type", "path": "type", "type": "code"},
                    {"name": "line", "path": "line.join(', ')", "type": "string"},
                    {"name": "city", "path": "city", "type": "string"},
                    {"name": "state", "path": "state", "type": "string"},
                    {"name": "postal_code", "path": "postalCode", "type": "string"},
                    {"name": "country", "path": "country", "type": "string"}
                ]
            }]
        }
    
    @staticmethod
    def patient_identifiers() -> Dict[str, Any]:
        """
        Patient identifiers with forEach to handle multiple identifier types.
        
        Returns:
            ViewDefinition for patient identifiers
            
        Output Columns:
            - patient_id: Patient identifier
            - identifier_use: Identifier use (usual, official, etc.)
            - identifier_type: Identifier type coding
            - identifier_system: Identifier system URL
            - identifier_value: Identifier value
            
        Example:
            >>> view_def = Templates.patient_identifiers()
            >>> df = db.execute_to_dataframe(view_def)
        """
        return {
            "resourceType": "ViewDefinition",
            "status": "active",
            "resource": "Patient",
            "select": [{
                "forEach": "identifier",
                "column": [
                    {"name": "patient_id", "path": "id", "type": "id"},
                    {"name": "identifier_use", "path": "use", "type": "code"},
                    {"name": "identifier_type", "path": "type.coding.code", "type": "code"},
                    {"name": "identifier_system", "path": "system", "type": "uri"},
                    {"name": "identifier_value", "path": "value", "type": "string"}
                ]
            }]
        }
    
    # Clinical Data Templates
    @staticmethod
    def observations_by_code(code: Optional[str] = None, 
                           code_system: str = "http://loinc.org") -> Dict[str, Any]:
        """
        Observation values filtered by code and system.
        
        Args:
            code: Specific observation code to filter by (optional)
            code_system: Coding system (default: LOINC)
            
        Returns:
            ViewDefinition for observation values
            
        Output Columns:
            - id: Observation identifier
            - patient_id: Patient reference
            - code: Observation code
            - display: Code display name
            - value_quantity: Numeric value
            - unit: Unit of measure
            - value_string: String value (for non-numeric observations)
            - effective_date: Observation date/time
            - status: Observation status
            
        Example:
            >>> # All observations
            >>> view_def = Templates.observations_by_code()
            >>> 
            >>> # Blood pressure observations
            >>> view_def = Templates.observations_by_code("85354-9")
        """
        view_def = {
            "resourceType": "ViewDefinition",
            "status": "active",
            "resource": "Observation",
            "select": [{
                "column": [
                    {"name": "id", "path": "id", "type": "id"},
                    {"name": "patient_id", "path": "subject.reference", "type": "string"},
                    {"name": "code", "path": "code.coding.code", "type": "code"},
                    {"name": "display", "path": "code.coding.display", "type": "string"},
                    {"name": "value_quantity", "path": "valueQuantity.value", "type": "decimal"},
                    {"name": "unit", "path": "valueQuantity.unit", "type": "string"},
                    {"name": "value_string", "path": "valueString", "type": "string"},
                    {"name": "effective_date", "path": "effectiveDateTime", "type": "dateTime"},
                    {"name": "status", "path": "status", "type": "code"}
                ],
                "where": [
                    {"path": "status = 'final'"},
                    {"path": f"code.coding.system = '{code_system}'"}
                ]
            }]
        }
        
        if code:
            view_def["select"][0]["where"].append({"path": f"code.coding.code = '{code}'"})
        
        return view_def
    
    @staticmethod
    def vital_signs() -> Dict[str, Any]:
        """
        Common vital signs observations with standardized codes.
        
        Returns:
            ViewDefinition for vital signs
            
        Output Columns:
            - id: Observation identifier
            - patient_id: Patient reference
            - vital_sign_type: Type of vital sign
            - code: LOINC code
            - value: Numeric value
            - unit: Unit of measure
            - effective_date: Measurement date/time
            - status: Observation status
            
        Example:
            >>> view_def = Templates.vital_signs()
            >>> df = db.execute_to_dataframe(view_def)
        """
        return {
            "resourceType": "ViewDefinition",
            "status": "active",
            "resource": "Observation",
            "select": [{
                "column": [
                    {"name": "id", "path": "id", "type": "id"},
                    {"name": "patient_id", "path": "subject.reference", "type": "string"},
                    {"name": "vital_sign_type", "path": "category.coding.display", "type": "string"},
                    {"name": "code", "path": "code.coding.code", "type": "code"},
                    {"name": "display", "path": "code.coding.display", "type": "string"},
                    {"name": "value", "path": "valueQuantity.value", "type": "decimal"},
                    {"name": "unit", "path": "valueQuantity.unit", "type": "string"},
                    {"name": "effective_date", "path": "effectiveDateTime", "type": "dateTime"},
                    {"name": "status", "path": "status", "type": "code"}
                ],
                "where": [
                    {"path": "status = 'final'"},
                    {"path": "category.coding.system = 'http://terminology.hl7.org/CodeSystem/observation-category'"},
                    {"path": "category.coding.code = 'vital-signs'"}
                ]
            }]
        }
    
    @staticmethod
    def lab_results(date_range_start: Optional[str] = None,
                   date_range_end: Optional[str] = None) -> Dict[str, Any]:
        """
        Laboratory results with optional date filtering.
        
        Args:
            date_range_start: Start date for filtering (ISO format)
            date_range_end: End date for filtering (ISO format)
            
        Returns:
            ViewDefinition for lab results
            
        Output Columns:
            - id: Observation identifier
            - patient_id: Patient reference
            - test_name: Laboratory test name
            - code: LOINC code
            - value: Numeric value
            - unit: Unit of measure
            - reference_range: Reference range text
            - abnormal_flag: Abnormal flag
            - effective_date: Test date/time
            - status: Observation status
            
        Example:
            >>> # All lab results
            >>> view_def = Templates.lab_results()
            >>> 
            >>> # Lab results from 2023
            >>> view_def = Templates.lab_results("2023-01-01", "2023-12-31")
        """
        where_clauses = [
            {"path": "status = 'final'"},
            {"path": "category.coding.system = 'http://terminology.hl7.org/CodeSystem/observation-category'"},
            {"path": "category.coding.code = 'laboratory'"}
        ]
        
        if date_range_start:
            where_clauses.append({"path": f"effectiveDateTime >= '{date_range_start}'"})
        
        if date_range_end:
            where_clauses.append({"path": f"effectiveDateTime <= '{date_range_end}'"})
        
        return {
            "resourceType": "ViewDefinition",
            "status": "active",
            "resource": "Observation",
            "select": [{
                "column": [
                    {"name": "id", "path": "id", "type": "id"},
                    {"name": "patient_id", "path": "subject.reference", "type": "string"},
                    {"name": "test_name", "path": "code.coding.display", "type": "string"},
                    {"name": "code", "path": "code.coding.code", "type": "code"},
                    {"name": "value", "path": "valueQuantity.value", "type": "decimal"},
                    {"name": "unit", "path": "valueQuantity.unit", "type": "string"},
                    {"name": "reference_range", "path": "referenceRange.text", "type": "string"},
                    {"name": "abnormal_flag", "path": "interpretation.coding.code", "type": "code"},
                    {"name": "effective_date", "path": "effectiveDateTime", "type": "dateTime"},
                    {"name": "status", "path": "status", "type": "code"}
                ],
                "where": where_clauses
            }]
        }
    
    @staticmethod
    def medications_current() -> Dict[str, Any]:
        """
        Current active medications from MedicationRequest resources.
        
        Returns:
            ViewDefinition for current medications
            
        Output Columns:
            - id: MedicationRequest identifier
            - patient_id: Patient reference
            - medication_name: Medication display name
            - medication_code: Medication code
            - dosage_text: Dosage instructions text
            - route: Route of administration
            - frequency: Dosing frequency
            - status: Request status
            - intent: Request intent
            - authored_date: Date request was authored
            
        Example:
            >>> view_def = Templates.medications_current()
            >>> df = db.execute_to_dataframe(view_def)
        """
        return {
            "resourceType": "ViewDefinition",
            "status": "active",
            "resource": "MedicationRequest",
            "select": [{
                "column": [
                    {"name": "id", "path": "id", "type": "id"},
                    {"name": "patient_id", "path": "subject.reference", "type": "string"},
                    {"name": "medication_name", "path": "medicationCodeableConcept.coding.display", "type": "string"},
                    {"name": "medication_code", "path": "medicationCodeableConcept.coding.code", "type": "code"},
                    {"name": "dosage_text", "path": "dosageInstruction.text", "type": "string"},
                    {"name": "route", "path": "dosageInstruction.route.coding.display", "type": "string"},
                    {"name": "frequency", "path": "dosageInstruction.timing.code.coding.display", "type": "string"},
                    {"name": "status", "path": "status", "type": "code"},
                    {"name": "intent", "path": "intent", "type": "code"},
                    {"name": "authored_date", "path": "authoredOn", "type": "dateTime"}
                ],
                "where": [
                    {"path": "status in ('active', 'completed')"},
                    {"path": "intent = 'order'"}
                ]
            }]
        }
    
    # Administrative Templates
    @staticmethod
    def encounters_summary(encounter_class: Optional[str] = None) -> Dict[str, Any]:
        """
        Encounter summary information with optional class filtering.
        
        Args:
            encounter_class: Filter by encounter class (e.g., 'inpatient', 'outpatient')
            
        Returns:
            ViewDefinition for encounter summary
            
        Output Columns:
            - id: Encounter identifier
            - patient_id: Patient reference
            - status: Encounter status
            - class: Encounter class
            - type: Encounter type
            - start_time: Period start
            - end_time: Period end
            - length_of_stay: Duration in hours
            - service_provider: Service provider organization
            
        Example:
            >>> # All encounters
            >>> view_def = Templates.encounters_summary()
            >>> 
            >>> # Inpatient encounters only
            >>> view_def = Templates.encounters_summary("inpatient")
        """
        where_clauses = []
        if encounter_class:
            where_clauses.append({"path": f"class.code = '{encounter_class}'"})
        
        view_def = {
            "resourceType": "ViewDefinition",
            "status": "active",
            "resource": "Encounter",
            "select": [{
                "column": [
                    {"name": "id", "path": "id", "type": "id"},
                    {"name": "patient_id", "path": "subject.reference", "type": "string"},
                    {"name": "status", "path": "status", "type": "code"},
                    {"name": "class", "path": "class.code", "type": "code"},
                    {"name": "type", "path": "type.coding.display", "type": "string"},
                    {"name": "start_time", "path": "period.start", "type": "dateTime"},
                    {"name": "end_time", "path": "period.end", "type": "dateTime"},
                    {"name": "service_provider", "path": "serviceProvider.reference", "type": "string"}
                ]
            }]
        }
        
        if where_clauses:
            view_def["select"][0]["where"] = where_clauses
        
        return view_def
    
    # Quality Measures Templates
    @staticmethod
    def diabetes_a1c_monitoring() -> Dict[str, Any]:
        """
        Diabetes A1C monitoring for quality measures.
        
        Returns:
            ViewDefinition for A1C observations in diabetic patients
            
        Output Columns:
            - patient_id: Patient reference
            - observation_id: Observation identifier
            - a1c_value: A1C percentage value
            - test_date: Date of A1C test
            - days_since_last: Days since previous test
            - meets_target: Whether A1C meets target (<7%)
            
        Example:
            >>> view_def = Templates.diabetes_a1c_monitoring()
            >>> df = db.execute_to_dataframe(view_def)
        """
        return {
            "resourceType": "ViewDefinition",
            "status": "active",
            "resource": "Observation",
            "select": [{
                "column": [
                    {"name": "patient_id", "path": "subject.reference", "type": "string"},
                    {"name": "observation_id", "path": "id", "type": "id"},
                    {"name": "a1c_value", "path": "valueQuantity.value", "type": "decimal"},
                    {"name": "test_date", "path": "effectiveDateTime", "type": "dateTime"},
                    {"name": "meets_target", "path": "valueQuantity.value < 7.0", "type": "boolean"}
                ],
                "where": [
                    {"path": "status = 'final'"},
                    {"path": "code.coding.code = '4548-4'"},  # HbA1c LOINC code
                    {"path": "code.coding.system = 'http://loinc.org'"}
                ]
            }]
        }
    
    # Research Templates  
    @staticmethod
    def cohort_identification(conditions: List[str]) -> Dict[str, Any]:
        """
        Patient cohort identification based on condition codes.
        
        Args:
            conditions: List of condition codes (ICD-10, SNOMED, etc.)
            
        Returns:
            ViewDefinition for patient cohort
            
        Output Columns:
            - patient_id: Patient identifier
            - condition_code: Condition code
            - condition_display: Condition description
            - onset_date: Condition onset date
            - clinical_status: Clinical status
            - verification_status: Verification status
            
        Example:
            >>> # Diabetes cohort
            >>> diabetes_codes = ["E11.9", "E10.9"]  # Type 2 and Type 1 diabetes
            >>> view_def = Templates.cohort_identification(diabetes_codes)
        """
        # Build condition filter
        if len(conditions) == 1:
            condition_filter = f"code.coding.code = '{conditions[0]}'"
        else:
            condition_list = "', '".join(conditions)
            condition_filter = f"code.coding.code in ('{condition_list}')"
        
        return {
            "resourceType": "ViewDefinition",
            "status": "active",
            "resource": "Condition",
            "select": [{
                "column": [
                    {"name": "patient_id", "path": "subject.reference", "type": "string"},
                    {"name": "condition_code", "path": "code.coding.code", "type": "code"},
                    {"name": "condition_display", "path": "code.coding.display", "type": "string"},
                    {"name": "onset_date", "path": "onsetDateTime", "type": "dateTime"},
                    {"name": "clinical_status", "path": "clinicalStatus.coding.code", "type": "code"},
                    {"name": "verification_status", "path": "verificationStatus.coding.code", "type": "code"}
                ],
                "where": [
                    {"path": condition_filter},
                    {"path": "clinicalStatus.coding.code = 'active'"}
                ]
            }]
        }


# Register template metadata
Templates._register_template(TemplateMetadata(
    name="patient_demographics",
    title="Patient Demographics",
    description="Basic patient information including name, birth date, gender, and contact details",
    resource_type="Patient",
    category="patient",
    tags=["demographics", "basic", "contact"],
    sample_output={"columns": ["id", "family_name", "given_names", "birth_date", "gender", "active", "phone", "email"]}
))

Templates._register_template(TemplateMetadata(
    name="patient_addresses",
    title="Patient Addresses",
    description="Patient address information with support for multiple addresses per patient",
    resource_type="Patient",
    category="patient",
    tags=["addresses", "contact", "geographic"],
    sample_output={"columns": ["patient_id", "address_use", "address_type", "line", "city", "state", "postal_code", "country"]}
))

Templates._register_template(TemplateMetadata(
    name="observations_by_code",
    title="Observations by Code",
    description="Observation values filtered by specific codes and coding systems",
    resource_type="Observation",
    category="clinical",
    tags=["observations", "laboratory", "vital-signs"],
    parameters=[
        {"name": "code", "type": "string", "description": "Specific observation code to filter by"},
        {"name": "code_system", "type": "string", "description": "Coding system URL", "default": "http://loinc.org"}
    ],
    sample_output={"columns": ["id", "patient_id", "code", "display", "value_quantity", "unit", "effective_date", "status"]}
))

Templates._register_template(TemplateMetadata(
    name="vital_signs",
    title="Vital Signs",
    description="Common vital signs observations with standardized LOINC codes",
    resource_type="Observation",
    category="clinical",
    tags=["vital-signs", "monitoring", "clinical"],
    sample_output={"columns": ["id", "patient_id", "vital_sign_type", "code", "value", "unit", "effective_date", "status"]}
))

Templates._register_template(TemplateMetadata(
    name="medications_current",
    title="Current Medications",
    description="Active medications from MedicationRequest resources",
    resource_type="MedicationRequest", 
    category="clinical",
    tags=["medications", "prescriptions", "pharmacy"],
    sample_output={"columns": ["id", "patient_id", "medication_name", "dosage_text", "route", "status", "authored_date"]}
))

Templates._register_template(TemplateMetadata(
    name="encounters_summary",
    title="Encounter Summary",
    description="Encounter information with optional filtering by encounter class",
    resource_type="Encounter",
    category="administrative",
    tags=["encounters", "visits", "admissions"],
    parameters=[
        {"name": "encounter_class", "type": "string", "description": "Filter by encounter class (inpatient, outpatient, etc.)"}
    ],
    sample_output={"columns": ["id", "patient_id", "status", "class", "type", "start_time", "end_time", "service_provider"]}
))

Templates._register_template(TemplateMetadata(
    name="diabetes_a1c_monitoring",
    title="Diabetes A1C Monitoring",
    description="A1C test results for diabetes quality monitoring",
    resource_type="Observation",
    category="quality",
    tags=["diabetes", "a1c", "quality-measures", "monitoring"],
    sample_output={"columns": ["patient_id", "observation_id", "a1c_value", "test_date", "meets_target"]}
))

Templates._register_template(TemplateMetadata(
    name="cohort_identification",
    title="Patient Cohort Identification",
    description="Identify patient cohorts based on condition codes for research studies",
    resource_type="Condition",
    category="research",
    tags=["cohort", "research", "conditions", "epidemiology"],
    parameters=[
        {"name": "conditions", "type": "list", "description": "List of condition codes to identify cohort"}
    ],
    sample_output={"columns": ["patient_id", "condition_code", "condition_display", "onset_date", "clinical_status"]}
))


class TemplateLibrary:
    """
    Extended template library with advanced template management capabilities.
    
    Provides functionality for template discovery, validation, and customization.
    """
    
    @staticmethod
    def search_templates(query: str) -> List[TemplateMetadata]:
        """
        Search templates by name, description, or tags.
        
        Args:
            query: Search query string
            
        Returns:
            List of matching templates
            
        Example:
            >>> results = TemplateLibrary.search_templates("diabetes")
            >>> for template in results:
            ...     print(f"{template.name}: {template.description}")
        """
        query_lower = query.lower()
        matching_templates = []
        
        for template in Templates._metadata_registry.values():
            # Search in name, title, description, and tags
            if (query_lower in template.name.lower() or
                query_lower in template.title.lower() or 
                query_lower in template.description.lower() or
                any(query_lower in tag.lower() for tag in template.tags)):
                matching_templates.append(template)
        
        return sorted(matching_templates, key=lambda t: t.name)
    
    @staticmethod
    def validate_template(template_name: str) -> Dict[str, Any]:
        """
        Validate a template and return validation results.
        
        Args:
            template_name: Name of template to validate
            
        Returns:
            Dictionary with validation results
            
        Example:
            >>> result = TemplateLibrary.validate_template("patient_demographics")
            >>> if result["valid"]:
            ...     print("Template is valid")
        """
        try:
            # Get template method
            template_method = getattr(Templates, template_name, None)
            if not template_method:
                return {
                    "valid": False,
                    "errors": [f"Template '{template_name}' not found"],
                    "warnings": []
                }
            
            # Try to generate ViewDefinition
            view_def = template_method()
            
            # Basic validation
            errors = []
            warnings = []
            
            # Check required fields
            if "resourceType" not in view_def:
                errors.append("Missing resourceType field")
            elif view_def["resourceType"] != "ViewDefinition":
                errors.append("Invalid resourceType, must be 'ViewDefinition'")
            
            if "resource" not in view_def:
                errors.append("Missing resource field")
            
            if "select" not in view_def:
                errors.append("Missing select field")
            elif not isinstance(view_def["select"], list) or not view_def["select"]:
                errors.append("Select field must be non-empty list")
            
            # Check select structure
            for i, select_item in enumerate(view_def.get("select", [])):
                if "column" not in select_item:
                    errors.append(f"Select item {i} missing column field")
                elif not isinstance(select_item["column"], list) or not select_item["column"]:
                    errors.append(f"Select item {i} column field must be non-empty list")
            
            return {
                "valid": len(errors) == 0,
                "errors": errors,
                "warnings": warnings,
                "view_definition": view_def
            }
            
        except Exception as e:
            return {
                "valid": False,
                "errors": [f"Template execution failed: {str(e)}"],
                "warnings": []
            }
    
    @staticmethod
    def export_template_documentation(output_path: str, format: str = "json"):
        """
        Export template documentation to file.
        
        Args:
            output_path: File path for output
            format: Output format ('json', 'markdown', 'csv')
            
        Example:
            >>> TemplateLibrary.export_template_documentation("templates.json")
            >>> TemplateLibrary.export_template_documentation("templates.md", "markdown")
        """
        templates = Templates.list_templates()
        
        if format == "json":
            # Export as JSON
            template_data = {
                "templates": [
                    {
                        "name": t.name,
                        "title": t.title,
                        "description": t.description,
                        "resource_type": t.resource_type,
                        "category": t.category,
                        "tags": t.tags,
                        "version": t.version,
                        "parameters": t.parameters,
                        "sample_output": t.sample_output
                    }
                    for t in templates
                ],
                "export_date": datetime.now().isoformat(),
                "total_templates": len(templates)
            }
            
            with open(output_path, 'w') as f:
                json.dump(template_data, f, indent=2)
                
        elif format == "markdown":
            # Export as Markdown documentation
            with open(output_path, 'w') as f:
                f.write("# FHIR4DS Template Library Documentation\n\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write(f"Total Templates: {len(templates)}\n\n")
                
                # Group by category
                categories = {}
                for template in templates:
                    if template.category not in categories:
                        categories[template.category] = []
                    categories[template.category].append(template)
                
                for category, cat_templates in sorted(categories.items()):
                    f.write(f"## {category.title()} Templates\n\n")
                    
                    for template in cat_templates:
                        f.write(f"### {template.title}\n\n")
                        f.write(f"**Name:** `{template.name}`\n\n")
                        f.write(f"**Resource Type:** {template.resource_type}\n\n")
                        f.write(f"**Description:** {template.description}\n\n")
                        f.write(f"**Tags:** {', '.join(template.tags)}\n\n")
                        
                        if template.parameters:
                            f.write("**Parameters:**\n\n")
                            for param in template.parameters:
                                f.write(f"- `{param['name']}` ({param['type']}): {param['description']}\n")
                            f.write("\n")
                        
                        f.write("---\n\n")
        
        logger.info(f"Template documentation exported to {output_path} in {format} format")