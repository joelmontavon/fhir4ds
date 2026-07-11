"""
Query Builder Module

Provides fluent API for building common FHIR queries without requiring deep ViewDefinition knowledge.
Includes method chaining, smart defaults, and auto-completion friendly design.

Examples:
    # Basic patient query
    query = (QueryBuilder()
        .resource("Patient")
        .columns(["id", "name.family", "birthDate"])
        .where("active = true")
        .build())
    
    # Complex observation query
    query = (QueryBuilder()
        .resource("Observation")
        .columns([
            {"name": "patient_id", "path": "subject.reference"},
            {"name": "code", "path": "code.coding.code"},
            {"name": "value", "path": "valueQuantity.value"},
            {"name": "unit", "path": "valueQuantity.unit"}
        ])
        .where("status = 'final'")
        .where("code.coding.system = 'http://loinc.org'")
        .build())
    
    # Using forEach for multi-value fields
    query = (QueryBuilder()
        .resource("Patient")
        .for_each("name")
        .columns([
            {"name": "patient_id", "path": "id"},
            {"name": "name_use", "path": "use"},
            {"name": "family_name", "path": "family"},
            {"name": "given_names", "path": "given.join(', ')"}
        ])
        .build())
"""

import logging
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ColumnDefinition:
    """
    Represents a column definition in a ViewDefinition.
    
    Attributes:
        name: Column name in the output
        path: FHIRPath expression to extract the value
        type: FHIR data type (optional, auto-detected if not specified)
        description: Human-readable description of the column
    """
    name: str
    path: str
    type: Optional[str] = None
    description: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to ViewDefinition column format."""
        result = {"name": self.name, "path": self.path}
        if self.type:
            result["type"] = self.type
        if self.description:
            result["description"] = self.description
        return result


@dataclass 
class WhereClause:
    """
    Represents a WHERE clause condition.
    
    Attributes:
        path: FHIRPath expression that evaluates to boolean
        description: Human-readable description of the condition
    """
    path: str
    description: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to ViewDefinition where format."""
        result = {"path": self.path}
        if self.description:
            result["description"] = self.description
        return result


class QueryBuilder:
    """
    Fluent API for building FHIR ViewDefinitions with method chaining.
    
    Provides an intuitive way to construct ViewDefinitions without requiring
    deep knowledge of the SQL-on-FHIR specification.
    """
    
    def __init__(self):
        """Initialize a new QueryBuilder."""
        self._resource: Optional[str] = None
        self._status: str = "active"
        self._columns: List[ColumnDefinition] = []
        self._where_clauses: List[WhereClause] = []
        self._for_each_path: Optional[str] = None
        self._for_each_or_null: bool = False
        self._constants: Dict[str, Any] = {}
        self._union_all: List['QueryBuilder'] = []
        
    def resource(self, resource_type: str) -> 'QueryBuilder':
        """
        Set the FHIR resource type for the query.
        
        Args:
            resource_type: FHIR resource type (e.g., "Patient", "Observation")
            
        Returns:
            QueryBuilder instance for method chaining
            
        Example:
            >>> builder.resource("Patient")
        """
        self._resource = resource_type
        return self
    
    def status(self, status: str) -> 'QueryBuilder':
        """
        Set the ViewDefinition status.
        
        Args:
            status: ViewDefinition status (default: "active")
            
        Returns:
            QueryBuilder instance for method chaining
        """
        self._status = status
        return self
    
    def column(self, name: str, path: str, type: Optional[str] = None, 
              description: Optional[str] = None) -> 'QueryBuilder':
        """
        Add a single column to the query.
        
        Args:
            name: Column name in the output
            path: FHIRPath expression to extract the value
            type: FHIR data type (optional)
            description: Human-readable description
            
        Returns:
            QueryBuilder instance for method chaining
            
        Example:
            >>> builder.column("patient_id", "id")
            >>> builder.column("birth_date", "birthDate", "date", "Patient birth date")
        """
        self._columns.append(ColumnDefinition(name, path, type, description))
        return self
    
    def columns(self, columns: List[Union[str, Dict[str, Any], ColumnDefinition]]) -> 'QueryBuilder':
        """
        Add multiple columns to the query.
        
        Args:
            columns: List of column specifications. Can be:
                - Strings (uses string as both name and path)
                - Dictionaries with 'name', 'path', and optional 'type', 'description'
                - ColumnDefinition objects
                
        Returns:
            QueryBuilder instance for method chaining
            
        Example:
            >>> builder.columns(["id", "name.family", "birthDate"])
            >>> builder.columns([
            ...     {"name": "patient_id", "path": "id"},
            ...     {"name": "family_name", "path": "name.family", "type": "string"}
            ... ])
        """
        for col in columns:
            if isinstance(col, str):
                # String shorthand: use as both name and path
                if '.' in col:
                    # For paths like "name.family", use the last part as name
                    name = col.split('.')[-1]
                else:
                    name = col
                self._columns.append(ColumnDefinition(name, col))
            elif isinstance(col, dict):
                # Dictionary format
                self._columns.append(ColumnDefinition(
                    name=col['name'],
                    path=col['path'],
                    type=col.get('type'),
                    description=col.get('description')
                ))
            elif isinstance(col, ColumnDefinition):
                # Direct ColumnDefinition object
                self._columns.append(col)
            else:
                raise ValueError(f"Unsupported column type: {type(col)}")
        
        return self
    
    def where(self, condition: str, description: Optional[str] = None) -> 'QueryBuilder':
        """
        Add a WHERE clause condition.
        
        Args:
            condition: FHIRPath expression that evaluates to boolean
            description: Human-readable description of the condition
            
        Returns:
            QueryBuilder instance for method chaining
            
        Example:
            >>> builder.where("active = true")
            >>> builder.where("birthDate > '1990-01-01'", "Born after 1990")
        """
        self._where_clauses.append(WhereClause(condition, description))
        return self
    
    def for_each(self, path: str, or_null: bool = False) -> 'QueryBuilder':
        """
        Add a forEach clause to iterate over array elements.
        
        Args:
            path: FHIRPath to the array to iterate over
            or_null: Whether to use forEachOrNull (includes null values)
            
        Returns:
            QueryBuilder instance for method chaining
            
        Example:
            >>> builder.for_each("name")  # Iterate over all names
            >>> builder.for_each("telecom", or_null=True)  # Include patients with no telecom
        """
        self._for_each_path = path
        self._for_each_or_null = or_null
        return self
    
    def constant(self, name: str, value: Any) -> 'QueryBuilder':
        """
        Add a constant value that can be referenced in expressions.
        
        Args:
            name: Constant name
            value: Constant value
            
        Returns:
            QueryBuilder instance for method chaining
            
        Example:
            >>> builder.constant("target_date", "2023-01-01")
            >>> builder.where("birthDate > %target_date")
        """
        self._constants[name] = value
        return self
    
    def union_all(self, other_builder: 'QueryBuilder') -> 'QueryBuilder':
        """
        Add a UNION ALL operation with another query.
        
        Args:
            other_builder: Another QueryBuilder to union with
            
        Returns:
            QueryBuilder instance for method chaining
            
        Example:
            >>> patients = QueryBuilder().resource("Patient").columns(["id", "name.family"])
            >>> practitioners = QueryBuilder().resource("Practitioner").columns(["id", "name.family"])
            >>> combined = patients.union_all(practitioners)
        """
        self._union_all.append(other_builder)
        return self
    
    def build(self) -> Dict[str, Any]:
        """
        Build the final ViewDefinition dictionary.
        
        Returns:
            Complete ViewDefinition that can be executed
            
        Raises:
            ValueError: If required fields are missing
            
        Example:
            >>> view_def = builder.build()
            >>> result = db.execute(view_def)
        """
        if not self._resource:
            raise ValueError("Resource type must be specified using .resource()")
        
        if not self._columns:
            raise ValueError("At least one column must be specified using .column() or .columns()")
        
        # Build base ViewDefinition
        view_def = {
            "resourceType": "ViewDefinition",
            "status": self._status,
            "resource": self._resource
        }
        
        # Add constants if any
        if self._constants:
            view_def["constant"] = [
                {"name": name, "value": value}
                for name, value in self._constants.items()
            ]
        
        # Build select structure
        select_structure = {
            "column": [col.to_dict() for col in self._columns]
        }
        
        # Add WHERE clauses if any
        if self._where_clauses:
            select_structure["where"] = [clause.to_dict() for clause in self._where_clauses]
        
        # Add forEach if specified
        if self._for_each_path:
            if self._for_each_or_null:
                select_structure["forEachOrNull"] = self._for_each_path
            else:
                select_structure["forEach"] = self._for_each_path
        
        # Handle union operations
        if self._union_all:
            # This query becomes the first select in unionAll
            union_selects = [select_structure]
            
            # Add other queries as additional selects
            for other_builder in self._union_all:
                other_view = other_builder.build()
                if "select" in other_view and other_view["select"]:
                    union_selects.extend(other_view["select"])
                else:
                    # Extract the select structure from the other builder
                    other_select = {
                        "column": [col.to_dict() for col in other_builder._columns]
                    }
                    if other_builder._where_clauses:
                        other_select["where"] = [clause.to_dict() for clause in other_builder._where_clauses]
                    union_selects.append(other_select)
            
            view_def["select"] = [{"unionAll": union_selects}]
        else:
            view_def["select"] = [select_structure]
        
        return view_def
    
    def validate(self) -> List[str]:
        """
        Validate the current builder configuration and return any issues.
        
        Returns:
            List of validation error messages (empty if valid)
            
        Example:
            >>> errors = builder.validate()
            >>> if errors:
            ...     print("Validation errors:", errors)
        """
        errors = []
        
        if not self._resource:
            errors.append("Resource type must be specified")
        
        if not self._columns:
            errors.append("At least one column must be specified")
        
        # Validate column names are unique
        column_names = [col.name for col in self._columns]
        if len(column_names) != len(set(column_names)):
            errors.append("Column names must be unique")
        
        # Validate FHIRPath expressions (basic check)
        for col in self._columns:
            if not col.path.strip():
                errors.append(f"Column '{col.name}' has empty path")
        
        for clause in self._where_clauses:
            if not clause.path.strip():
                errors.append("WHERE clause has empty path")
        
        return errors


class FHIRQueryBuilder:
    """
    FHIR-specific query builder with pre-configured patterns for common healthcare analytics.
    
    Provides specialized methods for common FHIR resource patterns and relationships.
    """
    
    @staticmethod
    def patient_demographics() -> QueryBuilder:
        """
        Build a query for basic patient demographics.
        
        Returns:
            QueryBuilder configured for patient demographics
            
        Example:
            >>> query = FHIRQueryBuilder.patient_demographics().build()
        """
        return (QueryBuilder()
            .resource("Patient")
            .columns([
                {"name": "id", "path": "id", "type": "id"},
                {"name": "family_name", "path": "name.family", "type": "string"},
                {"name": "given_names", "path": "name.given.join(', ')", "type": "string"},
                {"name": "birth_date", "path": "birthDate", "type": "date"},
                {"name": "gender", "path": "gender", "type": "code"},
                {"name": "active", "path": "active", "type": "boolean"}
            ]))
    
    @staticmethod
    def observation_values(code_system: str = "http://loinc.org", 
                          status: str = "final") -> QueryBuilder:
        """
        Build a query for observation values with filtering.
        
        Args:
            code_system: Coding system to filter by (default: LOINC)
            status: Observation status to filter by (default: "final")
            
        Returns:
            QueryBuilder configured for observation values
            
        Example:
            >>> query = FHIRQueryBuilder.observation_values().build()
        """
        builder = (QueryBuilder()
            .resource("Observation")
            .columns([
                {"name": "id", "path": "id", "type": "id"},
                {"name": "patient_id", "path": "subject.reference", "type": "string"},
                {"name": "code", "path": "code.coding.code", "type": "code"},
                {"name": "display", "path": "code.coding.display", "type": "string"},
                {"name": "value_quantity", "path": "valueQuantity.value", "type": "decimal"},
                {"name": "unit", "path": "valueQuantity.unit", "type": "string"},
                {"name": "effective_date", "path": "effectiveDateTime", "type": "dateTime"},
                {"name": "status", "path": "status", "type": "code"}
            ])
            .where(f"status = '{status}'"))
        
        if code_system:
            builder.where(f"code.coding.system = '{code_system}'")
        
        return builder
    
    @staticmethod
    def medication_list() -> QueryBuilder:
        """
        Build a query for patient medication list.
        
        Returns:
            QueryBuilder configured for medication information
            
        Example:
            >>> query = FHIRQueryBuilder.medication_list().build()
        """
        return (QueryBuilder()
            .resource("MedicationRequest")
            .columns([
                {"name": "id", "path": "id", "type": "id"},
                {"name": "patient_id", "path": "subject.reference", "type": "string"},
                {"name": "medication_code", "path": "medicationCodeableConcept.coding.code", "type": "code"},
                {"name": "medication_display", "path": "medicationCodeableConcept.coding.display", "type": "string"},
                {"name": "dosage_text", "path": "dosageInstruction.text", "type": "string"},
                {"name": "status", "path": "status", "type": "code"},
                {"name": "intent", "path": "intent", "type": "code"},
                {"name": "authored_on", "path": "authoredOn", "type": "dateTime"}
            ])
            .where("status = 'active' or status = 'completed'"))
    
    @staticmethod
    def patient_contacts() -> QueryBuilder:
        """
        Build a query for patient contact information.
        
        Returns:
            QueryBuilder configured for patient contacts using forEach
            
        Example:
            >>> query = FHIRQueryBuilder.patient_contacts().build()
        """
        return (QueryBuilder()
            .resource("Patient")
            .for_each("telecom")
            .columns([
                {"name": "patient_id", "path": "id", "type": "id"},
                {"name": "contact_system", "path": "system", "type": "code"},
                {"name": "contact_value", "path": "value", "type": "string"},
                {"name": "contact_use", "path": "use", "type": "code"}
            ]))
    
    @staticmethod
    def encounter_summary() -> QueryBuilder:
        """
        Build a query for encounter summary information.
        
        Returns:
            QueryBuilder configured for encounter data
            
        Example:
            >>> query = FHIRQueryBuilder.encounter_summary().build()
        """
        return (QueryBuilder()
            .resource("Encounter")
            .columns([
                {"name": "id", "path": "id", "type": "id"},
                {"name": "patient_id", "path": "subject.reference", "type": "string"},
                {"name": "status", "path": "status", "type": "code"},
                {"name": "class", "path": "class.code", "type": "code"},
                {"name": "type_code", "path": "type.coding.code", "type": "code"},
                {"name": "type_display", "path": "type.coding.display", "type": "string"},
                {"name": "start_time", "path": "period.start", "type": "dateTime"},
                {"name": "end_time", "path": "period.end", "type": "dateTime"}
            ]))
    
    @staticmethod
    def diagnostic_reports() -> QueryBuilder:
        """
        Build a query for diagnostic report information.
        
        Returns:
            QueryBuilder configured for diagnostic reports
            
        Example:
            >>> query = FHIRQueryBuilder.diagnostic_reports().build()
        """
        return (QueryBuilder()
            .resource("DiagnosticReport")
            .columns([
                {"name": "id", "path": "id", "type": "id"},
                {"name": "patient_id", "path": "subject.reference", "type": "string"},
                {"name": "code", "path": "code.coding.code", "type": "code"},
                {"name": "display", "path": "code.coding.display", "type": "string"},
                {"name": "status", "path": "status", "type": "code"},
                {"name": "effective_date", "path": "effectiveDateTime", "type": "dateTime"},
                {"name": "conclusion", "path": "conclusion", "type": "string"}
            ])
            .where("status = 'final'"))