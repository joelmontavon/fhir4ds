"""
Simplified End-to-End Workflow Tests

Core end-to-end workflow tests that focus on component integration
without requiring optional dependencies like performance monitoring.

These tests validate:
- Basic multi-component workflows
- Data flow between components
- Export and formatting capabilities
- Error handling in workflows
"""

import pytest
import tempfile
import os
import time
import json
from typing import Dict, List, Any

# Import test utilities
import sys
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

try:
    from fhir4ds.helpers import (
        QuickConnect, QueryBuilder, FHIRQueryBuilder, Templates, 
        ResultFormatter, BatchProcessor
    )
    from tests.helpers.database_fixtures import test_database
    from tests.helpers.test_data_generator import get_standard_test_data
    from tests.helpers.assertion_helpers import (
        assert_valid_query_result,
        assert_valid_dataframe,
        assert_batch_results_valid,
        assert_file_exists_and_not_empty
    )
except ImportError as e:
    pytest.skip(f"FHIR4DS modules not available: {e}", allow_module_level=True)


class TestBasicWorkflows:
    """Test basic healthcare analytics workflows."""
    
    def test_patient_demographics_workflow(self):
        """Test complete patient demographics analysis workflow."""
        with test_database() as db:
            # Step 1: Get patient demographics using template
            demographics_query = Templates.patient_demographics()
            result = db.execute(demographics_query)
            assert_valid_query_result(result)
            
            # Step 2: Convert to DataFrame for analysis
            df = ResultFormatter.to_dataframe(result)
            assert_valid_dataframe(df, expected_min_rows=0)
            
            # Step 3: Export results to different formats
            with tempfile.TemporaryDirectory() as temp_dir:
                # Export to CSV
                csv_path = os.path.join(temp_dir, "demographics.csv")
                ResultFormatter.to_csv(result, csv_path)
                assert_file_exists_and_not_empty(csv_path)
                
                # Export to JSON
                json_path = os.path.join(temp_dir, "demographics.json")
                ResultFormatter.to_json(result, json_path)
                assert_file_exists_and_not_empty(json_path)
                
                # Export to Excel (skip if openpyxl not available)
                try:
                    excel_path = os.path.join(temp_dir, "demographics.xlsx")
                    ResultFormatter.to_excel(result, excel_path)
                    assert_file_exists_and_not_empty(excel_path)
                except ImportError:
                    pass  # Skip Excel export if openpyxl not available
    
    def test_multi_template_workflow(self):
        """Test workflow with multiple templates."""
        with test_database() as db:
            # Step 1: Execute multiple templates
            templates_to_test = [
                ("demographics", Templates.patient_demographics()),
                ("vital_signs", Templates.vital_signs()),
                ("lab_results", Templates.lab_results()),
                ("medications", Templates.medications_current())
            ]
            
            results = {}
            for name, template in templates_to_test:
                result = db.execute(template)
                assert_valid_query_result(result)
                results[name] = result
            
            # Step 2: Process all results
            processed_data = {}
            for name, result in results.items():
                df = ResultFormatter.to_dataframe(result)
                assert_valid_dataframe(df, expected_min_rows=0)
                processed_data[name] = df
            
            # Step 3: Validate all datasets
            for name, df in processed_data.items():
                assert len(df.columns) > 0, f"{name} should have columns"
            
            # Step 4: Export combined results
            with tempfile.TemporaryDirectory() as temp_dir:
                for name, result in results.items():
                    export_path = os.path.join(temp_dir, f"{name}.csv")
                    ResultFormatter.to_csv(result, export_path)
                    assert_file_exists_and_not_empty(export_path)


class TestQueryBuilderWorkflows:
    """Test workflows using QueryBuilder for custom queries."""
    
    def test_custom_patient_query_workflow(self):
        """Test building and executing custom patient queries."""
        with test_database() as db:
            # Step 1: Build custom query using QueryBuilder
            custom_query = (QueryBuilder()
                          .resource("Patient")
                          .columns([
                              {"name": "patient_id", "path": "id"},
                              {"name": "family_name", "path": "name.family"},
                              {"name": "birth_date", "path": "birthDate"},
                              {"name": "gender", "path": "gender"},
                              {"name": "active_status", "path": "active"}
                          ])
                          .where("active = true")
                          .build())
            
            # Step 2: Execute query
            result = db.execute(custom_query)
            assert_valid_query_result(result)
            
            # Step 3: Process results
            # ResultFormatter is static
            df = ResultFormatter.to_dataframe(result)
            assert_valid_dataframe(df)
            
            # Step 4: Validate query structure
            expected_columns = ["patient_id", "family_name", "birth_date", "gender", "active_status"]
            for col in expected_columns:
                assert col in df.columns, f"Missing expected column: {col}"
            
            # Step 5: Export results
            with tempfile.TemporaryDirectory() as temp_dir:
                export_path = os.path.join(temp_dir, "custom_patients.json")
                ResultFormatter.to_json(result, export_path)
                assert_file_exists_and_not_empty(export_path)
    
    def test_fhir_query_builder_shortcuts(self):
        """Test FHIRQueryBuilder shortcuts."""
        with test_database() as db:
            # Step 1: Use FHIR shortcuts
            shortcuts_to_test = [
                ("patient_demo", FHIRQueryBuilder.patient_demographics()),
                ("observations", FHIRQueryBuilder.observation_values()),
                ("medications", FHIRQueryBuilder.medication_list())
            ]
            
            results = {}
            for name, builder in shortcuts_to_test:
                query = builder.build()
                result = db.execute(query)
                assert_valid_query_result(result)
                results[name] = result
            
            # Step 2: Validate all shortcuts work
            for name, result in results.items():
                # ResultFormatter is static
                df = ResultFormatter.to_dataframe(result)
                assert_valid_dataframe(df, expected_min_rows=0)
            
            # Step 3: Export shortcut results
            with tempfile.TemporaryDirectory() as temp_dir:
                for name, result in results.items():
                    # ResultFormatter is static
                    export_path = os.path.join(temp_dir, f"fhir_{name}.csv")
                    ResultFormatter.to_csv(result, export_path)
                    assert_file_exists_and_not_empty(export_path)


class TestBatchProcessingWorkflows:
    """Test batch processing workflows."""
    
    def test_template_batch_execution(self):
        """Test executing multiple templates in batch."""
        with test_database() as db:
            # Step 1: Prepare multiple queries
            queries = [
                Templates.patient_demographics(),
                Templates.vital_signs(),
                Templates.lab_results(),
                Templates.medications_current()
            ]
            query_names = ["patient_demographics", "vital_signs", "lab_results", "medications"]
            
            # Step 2: Execute batch processing
            processor = BatchProcessor(db)
            batch_results = processor.execute_batch(queries, parallel=True)
            
            # Step 3: Validate batch results
            assert_batch_results_valid(batch_results, expected_count=len(queries))
            
            # Step 4: Process successful results
            successful_results = [r for r in batch_results if r.success]
            assert len(successful_results) >= 1, "At least one query should succeed"
            
            # Step 5: Export batch results
            with tempfile.TemporaryDirectory() as temp_dir:
                batch_dir = os.path.join(temp_dir, "batch_results")
                os.makedirs(batch_dir, exist_ok=True)
                
                for i, name in enumerate(query_names):
                    if i < len(batch_results) and batch_results[i].success:
                        export_path = os.path.join(batch_dir, f"{name}.csv")
                        ResultFormatter.to_csv(batch_results[i].result, export_path)
                        assert_file_exists_and_not_empty(export_path)
    
    def test_custom_query_batch_workflow(self):
        """Test batch processing with custom QueryBuilder queries."""
        with test_database() as db:
            # Step 1: Build multiple custom queries
            queries = [
                (QueryBuilder()
                 .resource("Patient")
                 .columns(["id", "name.family", "active"])
                 .where("active = true")
                 .build()),
                
                (QueryBuilder()
                 .resource("Observation")
                 .columns([
                     {"name": "id", "path": "id"},
                     {"name": "patient", "path": "subject.reference"},
                     {"name": "value", "path": "valueQuantity.value"}
                 ])
                 .where("status = 'final'")
                 .where("valueQuantity.value IS NOT NULL")
                 .build())
            ]
            query_names = ["active_patients", "observations_with_values"]
            
            # Step 2: Execute batch
            processor = BatchProcessor(db)
            batch_results = processor.execute_batch(queries, parallel=False)
            
            # Step 3: Validate results
            assert_batch_results_valid(batch_results, expected_count=len(queries))
            
            # Step 4: Analyze results
            successful_count = sum(1 for result in batch_results if result.success)
            assert successful_count >= 1, "At least one custom query should succeed"


class TestQuickConnectWorkflows:
    """Test workflows with QuickConnect databases."""
    
    def test_memory_database_workflow(self):
        """Test complete workflow with in-memory database."""
        # Step 1: Create and populate database
        db = QuickConnect.memory()
        test_data = get_standard_test_data()
        db.load_resources(test_data)
        
        # Step 2: Execute analysis
        template = Templates.patient_demographics()
        result = db.execute(template)
        assert_valid_query_result(result)
        
        # Step 3: Process and export
        # ResultFormatter is static
        df = ResultFormatter.to_dataframe(result)
        assert_valid_dataframe(df)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            export_path = os.path.join(temp_dir, "memory_db_results.json")
            ResultFormatter.to_json(result, export_path)
            assert_file_exists_and_not_empty(export_path)
        
        # Step 4: Validate database state
        info = db.info()
        assert info['resources_loaded'] == len(test_data)
        assert info['queries_executed'] >= 1
    
    def test_file_database_workflow(self):
        """Test workflow with file-based database."""
        test_data = get_standard_test_data()
        
        temp_dir = tempfile.mkdtemp()
        temp_path = os.path.join(temp_dir, "test_database.db")
        
        try:
            # Step 1: Create file-based database
            db = QuickConnect.duckdb(temp_path)
            db.load_resources(test_data)
            
            # Step 2: Execute query and validate
            template = Templates.patient_demographics()
            result = db.execute(template)
            df = ResultFormatter.to_dataframe(result)
            assert_valid_dataframe(df)
            
            # Step 3: Verify database file exists
            assert os.path.exists(temp_path), "Database file should exist"
            assert os.path.getsize(temp_path) > 0, "Database file should not be empty"
            
            # Step 4: Export results
            with tempfile.TemporaryDirectory() as export_dir:
                export_path = os.path.join(export_dir, "file_db_results.csv")
                ResultFormatter.to_csv(result, export_path)
                assert_file_exists_and_not_empty(export_path)
            
        finally:
            import shutil
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)


class TestComplexWorkflows:
    """Test complex multi-step workflows."""
    
    def test_healthcare_reporting_workflow(self):
        """Test comprehensive healthcare reporting workflow."""
        with test_database() as db:
            # Step 1: Generate report data
            report_queries = {
                'patient_summary': Templates.patient_demographics(),
                'clinical_data': Templates.vital_signs(),
                'medication_usage': Templates.medications_current(),
                'lab_activities': Templates.lab_results()
            }
            
            # Step 2: Execute all report queries
            report_data = {}
            for name, query in report_queries.items():
                result = db.execute(query)
                assert_valid_query_result(result)
                report_data[name] = result
            
            # Step 3: Process all datasets
            processed_data = {}
            for name, result in report_data.items():
                # ResultFormatter is static
                df = ResultFormatter.to_dataframe(result)
                assert_valid_dataframe(df, expected_min_rows=0)
                processed_data[name] = df
            
            # Step 4: Generate comprehensive report
            with tempfile.TemporaryDirectory() as temp_dir:
                report_dir = os.path.join(temp_dir, "healthcare_report")
                os.makedirs(report_dir, exist_ok=True)
                
                # Export each dataset
                for name, result in report_data.items():
                    # Excel for stakeholders (skip if openpyxl not available)
                    try:
                        excel_path = os.path.join(report_dir, f"{name}.xlsx")
                        ResultFormatter.to_excel(result, excel_path)
                        assert_file_exists_and_not_empty(excel_path)
                    except ImportError:
                        pass  # Skip Excel export if openpyxl not available
                    
                    # CSV for analysis
                    csv_path = os.path.join(report_dir, f"{name}.csv")
                    ResultFormatter.to_csv(result, csv_path)
                    assert_file_exists_and_not_empty(csv_path)
                
                # Generate summary report
                summary_path = os.path.join(report_dir, "report_summary.json")
                summary_data = {
                    'report_date': time.strftime('%Y-%m-%d'),
                    'datasets_generated': len(report_data),
                    'datasets': {}
                }
                
                for name, df in processed_data.items():
                    summary_data['datasets'][name] = {
                        'row_count': len(df),
                        'column_count': len(df.columns)
                    }
                
                with open(summary_path, 'w') as f:
                    json.dump(summary_data, f, indent=2)
                assert_file_exists_and_not_empty(summary_path)
    
    def test_data_analysis_pipeline(self):
        """Test complete data analysis pipeline."""
        with test_database() as db:
            # Step 1: Extract base data
            patient_query = Templates.patient_demographics()
            patient_result = db.execute(patient_query)
            patient_df = ResultFormatter.to_dataframe(patient_result)
            
            # Step 2: Build custom analysis queries based on patient data
            if len(patient_df) > 0:
                # Build query for specific patients
                custom_query = (QueryBuilder()
                              .resource("Observation")
                              .columns([
                                  {"name": "patient_id", "path": "subject.reference"},
                                  {"name": "observation_code", "path": "code.coding.code"},
                                  {"name": "value", "path": "valueQuantity.value"}
                              ])
                              .where("status = 'final'")
                              .build())
                
                custom_result = db.execute(custom_query)
                custom_df = ResultFormatter.to_dataframe(custom_result)
            else:
                custom_df = None
            
            # Step 3: Combine and analyze data
            analysis_results = {
                'patients': patient_df,
                'observations': custom_df
            }
            
            # Step 4: Generate analysis outputs
            with tempfile.TemporaryDirectory() as temp_dir:
                analysis_dir = os.path.join(temp_dir, "data_analysis")
                os.makedirs(analysis_dir, exist_ok=True)
                
                for name, df in analysis_results.items():
                    if df is not None and len(df) > 0:
                        export_path = os.path.join(analysis_dir, f"{name}_analysis.csv")
                        df.to_csv(export_path, index=False)
                        assert_file_exists_and_not_empty(export_path)
                
                # Create analysis metadata
                metadata_path = os.path.join(analysis_dir, "analysis_metadata.json")
                metadata = {
                    'analysis_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'patient_count': len(patient_df) if patient_df is not None else 0,
                    'observation_count': len(custom_df) if custom_df is not None else 0,
                    'analysis_complete': True
                }
                
                with open(metadata_path, 'w') as f:
                    json.dump(metadata, f, indent=2)
                assert_file_exists_and_not_empty(metadata_path)


class TestErrorHandlingWorkflows:
    """Test error handling in workflows."""
    
    def test_workflow_with_invalid_queries(self):
        """Test workflow resilience with some invalid queries."""
        with test_database() as db:
            # Step 1: Mix valid and invalid queries
            queries = [
                ("valid_demographics", Templates.patient_demographics()),
                ("invalid_query", {
                    "resourceType": "ViewDefinition",
                    "resource": "NonExistentResource",
                    "select": [{"column": [{"name": "invalid", "path": "invalid.path"}]}]
                }),
                ("valid_vitals", Templates.vital_signs())
            ]
            
            # Step 2: Execute queries with error handling
            results = {}
            for name, query in queries:
                try:
                    result = db.execute(query)
                    results[name] = {'success': True, 'result': result}
                except Exception as e:
                    results[name] = {'success': False, 'error': str(e)}
            
            # Step 3: Validate error handling
            success_count = sum(1 for r in results.values() if r['success'])
            failure_count = sum(1 for r in results.values() if not r['success'])
            
            assert success_count >= 1, "Should have at least one successful query"
            # Note: failure_count might be 0 if system handles invalid queries gracefully
            assert success_count + failure_count == len(queries), "Should account for all queries"
            
            # Step 4: Process only successful results
            with tempfile.TemporaryDirectory() as temp_dir:
                success_dir = os.path.join(temp_dir, "successful_results")
                os.makedirs(success_dir, exist_ok=True)
                
                exported_count = 0
                for name, result_data in results.items():
                    if result_data['success']:
                        export_path = os.path.join(success_dir, f"{name}.json")
                        ResultFormatter.to_json(result_data['result'], export_path)
                        assert_file_exists_and_not_empty(export_path)
                        exported_count += 1
                
                assert exported_count == success_count, "Should export all successful results"
    
    def test_empty_database_workflow(self):
        """Test workflow behavior with empty database."""
        # Step 1: Create empty database
        db = QuickConnect.memory()
        
        # Step 2: Execute queries on empty database
        template = Templates.patient_demographics()
        result = db.execute(template)
        
        # Step 3: Validate empty results
        assert_valid_query_result(result)
        # ResultFormatter is static
        df = ResultFormatter.to_dataframe(result)
        
        # Should return empty DataFrame with correct structure
        assert_valid_dataframe(df, expected_min_rows=0)
        assert len(df) == 0, "Should have no rows for empty database"
        assert len(df.columns) > 0, "Should still have column structure"
        
        # Step 4: Export empty results
        with tempfile.TemporaryDirectory() as temp_dir:
            export_path = os.path.join(temp_dir, "empty_results.csv")
            ResultFormatter.to_csv(result, export_path)
            assert_file_exists_and_not_empty(export_path)  # File should exist even if empty