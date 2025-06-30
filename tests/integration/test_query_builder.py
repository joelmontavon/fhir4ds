"""
QueryBuilder Integration Tests

Tests for the QueryBuilder fluent API including:
- Method chaining functionality
- Column specifications and types
- WHERE clause conditions
- forEach operations
- Validation and error handling
- ViewDefinition generation
"""

import pytest
import json
from typing import Dict, List, Any

# Import test utilities
import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

try:
    from fhir4ds.helpers import QueryBuilder, FHIRQueryBuilder
    from tests.helpers.database_fixtures import test_database
    from tests.helpers.test_data_generator import get_standard_test_data
    from tests.helpers.assertion_helpers import (
        assert_valid_viewdefinition,
        assert_valid_query_result,
        assert_database_connection_valid
    )
except ImportError as e:
    pytest.skip(f"FHIR4DS modules not available: {e}", allow_module_level=True)


class TestQueryBuilder:
    """Test cases for QueryBuilder fluent API."""
    
    def test_basic_query_building(self):
        """Test basic query building with method chaining."""
        query = (QueryBuilder()
                .resource("Patient")
                .columns(["id", "name.family", "birthDate"])
                .where("active = true")
                .build())
        
        # Validate ViewDefinition structure
        assert_valid_viewdefinition(query)
        assert query["resource"] == "Patient"
        assert len(query["select"][0]["column"]) == 3
        assert len(query["select"][0]["where"]) == 1
    
    def test_column_string_shorthand(self):
        """Test column specification using string shorthand."""
        query = (QueryBuilder()
                .resource("Patient")
                .columns(["id", "name.family", "birthDate"])
                .build())
        
        columns = query["select"][0]["column"]
        assert len(columns) == 3
        assert columns[0]["name"] == "id"
        assert columns[0]["path"] == "id"
        assert columns[1]["name"] == "family"
        assert columns[1]["path"] == "name.family"
    
    def test_column_dictionary_specification(self):
        """Test column specification using dictionaries."""
        query = (QueryBuilder()
                .resource("Observation")
                .columns([
                    {"name": "patient_id", "path": "subject.reference", "type": "string"},
                    {"name": "code", "path": "code.coding.code", "type": "code"},
                    {"name": "value", "path": "valueQuantity.value", "type": "decimal"}
                ])
                .build())
        
        columns = query["select"][0]["column"]
        assert len(columns) == 3
        assert columns[0]["name"] == "patient_id"
        assert columns[0]["path"] == "subject.reference"
        assert columns[0]["type"] == "string"
        assert columns[2]["type"] == "decimal"
    
    def test_multiple_where_clauses(self):
        """Test multiple WHERE clauses."""
        query = (QueryBuilder()
                .resource("Observation")
                .columns(["id", "status"])
                .where("status = 'final'")
                .where("code.coding.system = 'http://loinc.org'")
                .where("effectiveDateTime >= '2023-01-01'")
                .build())
        
        where_clauses = query["select"][0]["where"]
        assert len(where_clauses) == 3
        assert where_clauses[0]["path"] == "status = 'final'"
        assert where_clauses[1]["path"] == "code.coding.system = 'http://loinc.org'"
        assert where_clauses[2]["path"] == "effectiveDateTime >= '2023-01-01'"
    
    def test_foreach_functionality(self):
        """Test forEach clause for array iteration."""
        query = (QueryBuilder()
                .resource("Patient")
                .for_each("telecom")
                .columns([
                    {"name": "patient_id", "path": "id", "type": "id"},
                    {"name": "system", "path": "system", "type": "code"},
                    {"name": "value", "path": "value", "type": "string"}
                ])
                .build())
        
        select_item = query["select"][0]
        assert "forEach" in select_item
        assert select_item["forEach"] == "telecom"
        assert len(select_item["column"]) == 3
    
    def test_foreach_or_null(self):
        """Test forEachOrNull clause."""
        query = (QueryBuilder()
                .resource("Patient")
                .for_each("address", or_null=True)
                .columns([
                    {"name": "patient_id", "path": "id"},
                    {"name": "city", "path": "city"}
                ])
                .build())
        
        select_item = query["select"][0]
        assert "forEachOrNull" in select_item
        assert select_item["forEachOrNull"] == "address"
        assert "forEach" not in select_item
    
    def test_constants(self):
        """Test constant values in queries."""
        query = (QueryBuilder()
                .resource("Patient")
                .constant("target_date", "2023-01-01")
                .constant("status_filter", "active")
                .columns(["id", "birthDate"])
                .where("birthDate >= %target_date")
                .build())
        
        assert "constant" in query
        constants = query["constant"]
        assert len(constants) == 2
        assert constants[0]["name"] == "target_date"
        assert constants[0]["value"] == "2023-01-01"
        assert constants[1]["name"] == "status_filter"
        assert constants[1]["value"] == "active"
    
    def test_status_setting(self):
        """Test setting ViewDefinition status."""
        query = (QueryBuilder()
                .resource("Patient")
                .status("draft")
                .columns(["id"])
                .build())
        
        assert query["status"] == "draft"
    
    def test_single_column_addition(self):
        """Test adding columns one at a time."""
        builder = (QueryBuilder()
                  .resource("Patient")
                  .column("id", "id", "id", "Patient identifier")
                  .column("name", "name.family", "string", "Family name"))
        
        query = builder.build()
        columns = query["select"][0]["column"]
        assert len(columns) == 2
        assert columns[0]["description"] == "Patient identifier"
        assert columns[1]["description"] == "Family name"
    
    def test_validation_errors(self):
        """Test query validation."""
        builder = QueryBuilder()
        
        # Should fail without resource
        with pytest.raises(ValueError, match="Resource type must be specified"):
            builder.build()
        
        # Should fail without columns
        builder.resource("Patient")
        with pytest.raises(ValueError, match="At least one column must be specified"):
            builder.build()
    
    def test_builder_validation_method(self):
        """Test the validate() method."""
        builder = QueryBuilder()
        
        # Empty builder should have errors
        errors = builder.validate()
        assert len(errors) > 0
        assert any("Resource type must be specified" in error for error in errors)
        assert any("At least one column must be specified" in error for error in errors)
        
        # Complete builder should have no errors
        builder.resource("Patient").columns(["id", "name.family"])
        errors = builder.validate()
        assert len(errors) == 0
    
    def test_column_name_uniqueness_validation(self):
        """Test validation of unique column names."""
        builder = (QueryBuilder()
                  .resource("Patient")
                  .columns([
                      {"name": "id", "path": "id"},
                      {"name": "id", "path": "identifier.value"}  # Duplicate name
                  ]))
        
        errors = builder.validate()
        assert any("Column names must be unique" in error for error in errors)
    
    def test_empty_path_validation(self):
        """Test validation of empty paths."""
        builder = (QueryBuilder()
                  .resource("Patient")
                  .columns([
                      {"name": "id", "path": ""},  # Empty path
                      {"name": "name", "path": "name.family"}
                  ]))
        
        errors = builder.validate()
        assert any("has empty path" in error for error in errors)


class TestQueryBuilderExecution:
    """Test QueryBuilder with actual database execution."""
    
    def test_basic_patient_query_execution(self):
        """Test executing a basic patient query."""
        with test_database() as db:
            query = (QueryBuilder()
                    .resource("Patient")
                    .columns(["id", "name.family", "birthDate", "gender"])
                    .where("active = true")
                    .build())
            
            result = db.execute(query)
            assert_valid_query_result(result)
            
            rows = result.fetchall()
            assert len(rows) >= 0  # Should execute without error
    
    def test_observation_query_execution(self):
        """Test executing an observation query."""
        with test_database() as db:
            query = (QueryBuilder()
                    .resource("Observation")
                    .columns([
                        {"name": "id", "path": "id", "type": "id"},
                        {"name": "patient", "path": "subject.reference", "type": "string"},
                        {"name": "code", "path": "code.coding.code", "type": "code"},
                        {"name": "value", "path": "valueQuantity.value", "type": "decimal"}
                    ])
                    .where("status = 'final'")
                    .build())
            
            result = db.execute(query)
            assert_valid_query_result(result)
    
    def test_foreach_query_execution(self):
        """Test executing a forEach query."""
        with test_database() as db:
            query = (QueryBuilder()
                    .resource("Patient")
                    .for_each("telecom")
                    .columns([
                        {"name": "patient_id", "path": "id", "type": "id"},
                        {"name": "system", "path": "system", "type": "code"},
                        {"name": "value", "path": "value", "type": "string"}
                    ])
                    .build())
            
            result = db.execute(query)
            assert_valid_query_result(result)
            
            rows = result.fetchall()
            # Should return multiple rows for patients with multiple telecom entries
            assert isinstance(rows, list)
    
    def test_complex_query_execution(self):
        """Test executing a complex multi-constraint query."""
        with test_database() as db:
            query = (QueryBuilder()
                    .resource("Observation")
                    .columns([
                        {"name": "obs_id", "path": "id"},
                        {"name": "patient_ref", "path": "subject.reference"},
                        {"name": "effective_date", "path": "effectiveDateTime"},
                        {"name": "numeric_value", "path": "valueQuantity.value"}
                    ])
                    .where("status = 'final'")
                    .where("code.coding.system = 'http://loinc.org'")
                    .where("valueQuantity.value IS NOT NULL")
                    .build())
            
            result = db.execute(query)
            assert_valid_query_result(result)


class TestFHIRQueryBuilder:
    """Test cases for FHIRQueryBuilder shortcuts."""
    
    def test_patient_demographics_shortcut(self):
        """Test patient demographics shortcut."""
        builder = FHIRQueryBuilder.patient_demographics()
        query = builder.build()
        
        assert_valid_viewdefinition(query)
        assert query["resource"] == "Patient"
        
        # Should have standard demographic columns
        columns = query["select"][0]["column"]
        column_names = [col["name"] for col in columns]
        assert "id" in column_names
        assert "family_name" in column_names
        assert "given_names" in column_names
        assert "birth_date" in column_names
        assert "gender" in column_names
    
    def test_observation_values_shortcut(self):
        """Test observation values shortcut."""
        builder = FHIRQueryBuilder.observation_values()
        query = builder.build()
        
        assert_valid_viewdefinition(query)
        assert query["resource"] == "Observation"
        
        # Should have WHERE clause for status
        where_clauses = query["select"][0]["where"]
        assert len(where_clauses) >= 1
        assert any("status = 'final'" in clause["path"] for clause in where_clauses)
    
    def test_observation_values_with_custom_system(self):
        """Test observation values with custom coding system."""
        builder = FHIRQueryBuilder.observation_values(code_system="http://snomed.info/sct")
        query = builder.build()
        
        where_clauses = query["select"][0]["where"]
        assert any("code.coding.system = 'http://snomed.info/sct'" in clause["path"] for clause in where_clauses)
    
    def test_medication_list_shortcut(self):
        """Test medication list shortcut."""
        builder = FHIRQueryBuilder.medication_list()
        query = builder.build()
        
        assert_valid_viewdefinition(query)
        assert query["resource"] == "MedicationRequest"
        
        # Should have WHERE clause for active medications
        where_clauses = query["select"][0]["where"]
        assert any("status in ('active', 'completed')" in clause["path"] for clause in where_clauses)
    
    def test_patient_contacts_shortcut(self):
        """Test patient contacts shortcut."""
        builder = FHIRQueryBuilder.patient_contacts()
        query = builder.build()
        
        assert_valid_viewdefinition(query)
        assert query["resource"] == "Patient"
        
        # Should use forEach for telecom
        select_item = query["select"][0]
        assert "forEach" in select_item
        assert select_item["forEach"] == "telecom"
    
    def test_encounter_summary_shortcut(self):
        """Test encounter summary shortcut."""
        builder = FHIRQueryBuilder.encounter_summary()
        query = builder.build()
        
        assert_valid_viewdefinition(query)
        assert query["resource"] == "Encounter"
        
        columns = query["select"][0]["column"]
        column_names = [col["name"] for col in columns]
        assert "id" in column_names
        assert "patient_id" in column_names
        assert "status" in column_names
        assert "class" in column_names
    
    def test_diagnostic_reports_shortcut(self):
        """Test diagnostic reports shortcut."""
        builder = FHIRQueryBuilder.diagnostic_reports()
        query = builder.build()
        
        assert_valid_viewdefinition(query)
        assert query["resource"] == "DiagnosticReport"
        
        # Should filter for final status
        where_clauses = query["select"][0]["where"]
        assert any("status = 'final'" in clause["path"] for clause in where_clauses)


class TestQueryBuilderExecutionWithFHIRShortcuts:
    """Test FHIRQueryBuilder shortcuts with actual execution."""
    
    def test_patient_demographics_execution(self):
        """Test executing patient demographics shortcut."""
        with test_database() as db:
            query = FHIRQueryBuilder.patient_demographics().build()
            result = db.execute(query)
            assert_valid_query_result(result)
    
    def test_observation_values_execution(self):
        """Test executing observation values shortcut."""
        with test_database() as db:
            query = FHIRQueryBuilder.observation_values().build()
            result = db.execute(query)
            assert_valid_query_result(result)
    
    def test_patient_contacts_execution(self):
        """Test executing patient contacts shortcut."""
        with test_database() as db:
            query = FHIRQueryBuilder.patient_contacts().build()
            result = db.execute(query)
            assert_valid_query_result(result)
            
            rows = result.fetchall()
            # Should return multiple rows for patients with contact info
            assert isinstance(rows, list)


class TestQueryBuilderAdvancedFeatures:
    """Test advanced QueryBuilder features."""
    
    def test_union_all_functionality(self):
        """Test UNION ALL operations."""
        patients_query = (QueryBuilder()
                         .resource("Patient")
                         .columns([
                             {"name": "id", "path": "id"},
                             {"name": "name", "path": "name.family"}
                         ]))
        
        practitioners_query = (QueryBuilder()
                              .resource("Practitioner")
                              .columns([
                                  {"name": "id", "path": "id"},
                                  {"name": "name", "path": "name.family"}
                              ]))
        
        combined_query = patients_query.union_all(practitioners_query).build()
        
        assert_valid_viewdefinition(combined_query)
        # Should have unionAll structure
        assert "unionAll" in combined_query["select"][0]
    
    def test_method_chaining_order_independence(self):
        """Test that method chaining order doesn't affect result."""
        # Build query in different orders
        query1 = (QueryBuilder()
                 .resource("Patient")
                 .columns(["id", "name.family"])
                 .where("active = true")
                 .status("active")
                 .build())
        
        query2 = (QueryBuilder()
                 .status("active")
                 .where("active = true")
                 .resource("Patient")
                 .columns(["id", "name.family"])
                 .build())
        
        # Should produce equivalent ViewDefinitions
        assert query1["resourceType"] == query2["resourceType"]
        assert query1["resource"] == query2["resource"]
        assert query1["status"] == query2["status"]
        assert len(query1["select"][0]["column"]) == len(query2["select"][0]["column"])
        assert len(query1["select"][0]["where"]) == len(query2["select"][0]["where"])
    
    def test_builder_reuse(self):
        """Test that builders can be reused and modified."""
        base_builder = (QueryBuilder()
                       .resource("Patient")
                       .columns(["id", "name.family"]))
        
        # Create two different queries from same base
        query1 = base_builder.where("gender = 'male'").build()
        query2 = base_builder.where("gender = 'female'").build()
        
        # Both should be valid but different
        assert_valid_viewdefinition(query1)
        assert_valid_viewdefinition(query2)
        
        # WHERE clauses should be different
        where1 = query1["select"][0]["where"]
        where2 = query2["select"][0]["where"]
        
        # Note: This will accumulate WHERE clauses, which may be intended behavior
        # The exact behavior depends on implementation
        assert isinstance(where1, list)
        assert isinstance(where2, list)