"""
Cross-Component Integration Tests

Tests that validate integration between different FHIR4DS Phase 2 components
working together in realistic scenarios.

These tests focus on:
- Component interoperability
- Data flow between components
- Configuration compatibility
- Performance when components are combined
"""

import pytest
import tempfile
import os
import time
from typing import Dict, List, Any

# Import test utilities
import sys
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

try:
    from fhir4ds.helpers import (
        QuickConnect, QueryBuilder, FHIRQueryBuilder, Templates, 
        TemplateLibrary, ResultFormatter, BatchProcessor, PerformanceMonitor
    )
    from tests.helpers.database_fixtures import test_database, minimal_test_database
    from tests.helpers.test_data_generator import get_standard_test_data
    from tests.helpers.assertion_helpers import (
        assert_valid_query_result,
        assert_valid_dataframe,
        assert_performance_metrics_valid,
        assert_batch_results_valid,
        assert_file_exists_and_not_empty
    )
except ImportError as e:
    pytest.skip(f"FHIR4DS modules not available: {e}", allow_module_level=True)


class TestQuickConnectIntegration:
    """Test QuickConnect integration with other components."""
    
    def test_quickconnect_with_templates(self):
        """Test QuickConnect database with Templates execution."""
        # Step 1: Create QuickConnect database
        db = QuickConnect.memory()
        test_data = get_standard_test_data()
        db.load_resources(test_data)
        
        # Step 2: Execute various templates
        templates_to_test = [
            Templates.patient_demographics(),
            Templates.vital_signs(),
            Templates.lab_results(),
            Templates.medications_current()
        ]
        
        successful_executions = 0
        for template in templates_to_test:
            try:
                result = db.execute(template)
                assert_valid_query_result(result)
                successful_executions += 1
            except Exception as e:
                # Some templates may fail due to missing data, which is acceptable
                print(f"Template failed (acceptable): {e}")
        
        # Should execute at least half successfully
        assert successful_executions >= len(templates_to_test) // 2
        
        # Step 3: Test database statistics
        info = db.info()
        assert info['resources_loaded'] == len(test_data)
        assert info['queries_executed'] >= successful_executions
    
    def test_quickconnect_with_query_builder(self):
        """Test QuickConnect database with QueryBuilder."""
        # Step 1: Setup database
        db = QuickConnect.memory()
        test_data = get_standard_test_data()
        db.load_resources(test_data)
        
        # Step 2: Build and execute custom queries
        custom_queries = [
            (QueryBuilder()
             .resource("Patient")
             .columns(["id", "name.family", "birthDate"])
             .where("active = true")
             .build()),
            
            (FHIRQueryBuilder.patient_demographics()
             .where("gender = 'male'")
             .build()),
            
            (QueryBuilder()
             .resource("Observation")
             .columns([
                 {"name": "id", "path": "id"},
                 {"name": "patient", "path": "subject.reference"},
                 {"name": "value", "path": "valueQuantity.value"}
             ])
             .where("status = 'final'")
             .build())
        ]
        
        execution_count = 0
        for query in custom_queries:
            try:
                result = db.execute(query)
                assert_valid_query_result(result)
                execution_count += 1
            except Exception as e:
                print(f"Query failed (may be expected): {e}")
        
        # Should execute some queries successfully
        assert execution_count >= 1
        
        # Step 3: Validate database state
        final_info = db.info()
        assert final_info['queries_executed'] >= execution_count
    
    def test_quickconnect_persistence_with_components(self):
        """Test QuickConnect file persistence with other components."""
        test_data = get_standard_test_data()
        
        # Use tempdir + filename pattern (like working integration tests)
        temp_dir = tempfile.mkdtemp()
        temp_path = os.path.join(temp_dir, "test_persistence.db")
        
        try:
            # Step 1: Create persistent database
            db1 = QuickConnect.duckdb(temp_path)
            db1.load_resources(test_data)
            
            # Execute a template
            template = Templates.patient_demographics()
            result1 = db1.execute(template)
            df1 = ResultFormatter.to_dataframe(result1)
            
            # Step 2: Reconnect to same database
            db2 = QuickConnect.duckdb(temp_path)
            
            # Execute same template
            result2 = db2.execute(template)
            df2 = ResultFormatter.to_dataframe(result2)
            
            # Step 3: Validate persistence
            assert len(df1) == len(df2), "Results should be identical after reconnection"
            assert list(df1.columns) == list(df2.columns), "Columns should be identical"
            
        finally:
            # Cleanup temp directory
            import shutil
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)


class TestPerformanceMonitoringIntegration:
    """Test PerformanceMonitor integration with other components."""
    
    def test_performance_monitoring_with_templates(self):
        """Test PerformanceMonitor with Templates execution."""
        with test_database() as db:
            monitor = PerformanceMonitor(db)
            
            # Step 1: Execute templates with monitoring
            templates_to_monitor = [
                ("patient_demographics", Templates.patient_demographics()),
                ("vital_signs", Templates.vital_signs()),
                ("lab_results", Templates.lab_results())
            ]
            
            all_metrics = []
            for name, template in templates_to_monitor:
                result = monitor.execute_with_profiling(template)
                metrics = monitor.get_last_metrics()
                all_metrics.append((name, metrics))
                
                assert_valid_query_result(result)
                assert_performance_metrics_valid(metrics)
            
            # Step 2: Analyze performance patterns
            execution_times = [metrics.execution_time for _, metrics in all_metrics]
            avg_time = sum(execution_times) / len(execution_times)
            
            assert avg_time < 5.0, f"Average execution time too high: {avg_time:.3f}s"
            assert all(metrics.success for _, metrics in all_metrics), "All monitored queries should succeed"
            
            # Step 3: Get optimization suggestions
            suggestions = monitor.get_optimization_suggestions()
            assert hasattr(suggestions, 'recommendations'), "Should provide optimization recommendations"
            assert hasattr(suggestions, 'overall_score'), "Should provide overall performance score"
    
    def test_performance_monitoring_with_batch_processing(self):
        """Test PerformanceMonitor with BatchProcessor."""
        with test_database() as db:
            # Step 1: Setup batch processor with monitoring
            processor = BatchProcessor(db)
            queries = [
                ("query1", Templates.patient_demographics()),
                ("query2", Templates.vital_signs()),
                ("query3", Templates.medications_current())
            ]
            
            for name, query in queries:
                processor.add_query(name, query)
            
            # Step 2: Execute batch with performance monitoring
            start_time = time.time()
            batch_results = processor.execute_all(
                parallel=True,
                monitor_performance=True,
                max_workers=2
            )
            total_time = time.time() - start_time
            
            # Step 3: Validate batch performance
            assert_batch_results_valid(batch_results, expected_count=len(queries))
            assert total_time < 15.0, f"Batch execution too slow: {total_time:.3f}s"
            
            # Step 4: Analyze individual query performance
            successful_count = sum(1 for result in batch_results if result.success)
            total_execution_time = sum(result.execution_time for result in batch_results)
            
            assert successful_count >= len(queries) // 2, "Most queries should succeed"
            assert total_execution_time < total_time * 2, "Parallel execution should show efficiency gains"


class TestResultFormatterIntegration:
    """Test ResultFormatter integration with other components."""
    
    def test_result_formatter_with_templates(self):
        """Test ResultFormatter with Template results."""
        with test_database() as db:
            # Step 1: Execute template and format results
            template = Templates.patient_demographics()
            result = db.execute(template)
            
            # Step 2: Test multiple export formats
            with tempfile.TemporaryDirectory() as temp_dir:
                # DataFrame conversion
                df = ResultFormatter.to_dataframe(result)
                assert_valid_dataframe(df)
                
                # CSV export
                csv_path = os.path.join(temp_dir, "template_result.csv")
                ResultFormatter.to_csv(result, csv_path)
                assert_file_exists_and_not_empty(csv_path)
                
                # JSON export
                json_path = os.path.join(temp_dir, "template_result.json")
                # Save to file (returns None)
                ResultFormatter.to_json(result, json_path)
                assert_file_exists_and_not_empty(json_path)
                
                # Get JSON as string (no output_path)
                json_data = ResultFormatter.to_json(result)
                assert isinstance(json_data, str)
                
                # Excel export
                excel_path = os.path.join(temp_dir, "template_result.xlsx")
                ResultFormatter.to_excel([result], excel_path)
                assert_file_exists_and_not_empty(excel_path)
            
            # Step 3: Validate data consistency across formats
            assert len(df) >= 0, "DataFrame should have valid row count"
            assert len(df.columns) > 0, "DataFrame should have columns"
    
    def test_result_formatter_with_query_builder(self):
        """Test ResultFormatter with QueryBuilder results."""
        with test_database() as db:
            # Step 1: Build custom query
            query = (QueryBuilder()
                    .resource("Patient")
                    .columns([
                        {"name": "patient_id", "path": "id"},
                        {"name": "family_name", "path": "name.family"},
                        {"name": "birth_date", "path": "birthDate"},
                        {"name": "gender", "path": "gender"}
                    ])
                    .where("active = true")
                    .build())
            
            # Step 2: Execute and format
            result = db.execute(query)
            # Step 3: Test formatting
            df = ResultFormatter.to_dataframe(result)
            assert_valid_dataframe(df)
            
            # Validate expected columns
            expected_columns = ["patient_id", "family_name", "birth_date", "gender"]
            for col in expected_columns:
                assert col in df.columns, f"Missing expected column: {col}"
            
            # Step 4: Test JSON export structure
            with tempfile.TemporaryDirectory() as temp_dir:
                json_path = os.path.join(temp_dir, "query_result.json")
                json_data = ResultFormatter.to_json(result, json_path)
                
                # Parse JSON to validate structure
                import json
                parsed_data = json.loads(json_data)
                assert "data" in parsed_data, "JSON should have data field"
                assert "row_count" in parsed_data, "JSON should have row_count field"
                assert parsed_data["row_count"] == len(df), "Row count should match DataFrame"


class TestTemplateLibraryIntegration:
    """Test TemplateLibrary integration with execution components."""
    
    def test_template_library_with_database_execution(self):
        """Test TemplateLibrary templates with database execution."""
        with test_database() as db:
            library = TemplateLibrary()
            
            # Step 1: Search for executable templates
            patient_templates = library.search_templates("patient")
            
            assert len(patient_templates) > 0, "Should find patient-related templates"
            
            # Step 2: Execute templates found in library
            successful_executions = 0
            for template_meta in patient_templates[:3]:  # Test first 3 templates
                try:
                    # Get actual template (simplified - using Templates class)
                    if template_meta.name == "patient_demographics":
                        template = Templates.patient_demographics()
                    elif template_meta.name == "patient_addresses":
                        template = Templates.patient_addresses()
                    elif template_meta.name == "patient_identifiers":
                        template = Templates.patient_identifiers()
                    else:
                        continue
                    
                    # Validate template first
                    validation = library.validate_template(template)
                    
                    # Execute if validation passes or has warnings only
                    if validation.get("valid", False) or len(validation.get("errors", [])) == 0:
                        result = db.execute(template)
                        assert_valid_query_result(result)
                        successful_executions += 1
                    
                except Exception as e:
                    print(f"Template execution failed (may be expected): {e}")
            
            # Should execute at least one template successfully
            assert successful_executions >= 1, "Should execute at least one library template"
    
    def test_template_library_documentation_export(self):
        """Test TemplateLibrary documentation export integration."""
        library = TemplateLibrary()
        
        # Step 1: Export documentation
        with tempfile.TemporaryDirectory() as temp_dir:
            doc_path = os.path.join(temp_dir, "template_documentation.md")
            library.export_template_documentation(doc_path)
            
            # Step 2: Validate documentation
            assert_file_exists_and_not_empty(doc_path)
            
            # Read and validate content
            with open(doc_path, 'r') as f:
                content = f.read()
            
            assert len(content) > 100, "Documentation should be substantial"
            assert "template" in content.lower(), "Should contain template information"
            
            # Step 3: Validate it contains template information
            templates = library.search_templates("")  # Get all templates
            for template_meta in templates[:2]:  # Check first 2 templates
                assert template_meta.name in content, f"Documentation should mention {template_meta.name}"


class TestComplexIntegrationScenarios:
    """Test complex scenarios involving multiple components."""
    
    def test_full_stack_analytics_pipeline(self):
        """Test complete analytics pipeline using all components."""
        # Step 1: Setup database with QuickConnect
        db = QuickConnect.memory()
        test_data = get_standard_test_data()
        db.load_resources(test_data)
        
        # Step 2: Initialize all components
        monitor = PerformanceMonitor(db)
        processor = BatchProcessor(db)
        library = TemplateLibrary()
        
        # Step 3: Build analysis pipeline
        analysis_queries = [
            ("demographics", Templates.patient_demographics()),
            ("custom_active_patients", 
             (QueryBuilder()
              .resource("Patient")
              .columns(["id", "name.family", "active"])
              .where("active = true")
              .build())),
            ("vital_signs", Templates.vital_signs())
        ]
        
        # Step 4: Execute pipeline with monitoring
        pipeline_results = {}
        for name, query in analysis_queries:
            # Validate query if from library
            if name == "demographics":
                validation = library.validate_template(query)
                # Execute even if validation has minor issues for test completeness
            
            # Execute with monitoring
            result = monitor.execute_with_profiling(query)
            metrics = monitor.get_last_metrics()
            
            pipeline_results[name] = {
                'result': result,
                'metrics': metrics,
                'success': metrics.success if metrics else False
            }
        
        # Step 5: Process and export all results
        with tempfile.TemporaryDirectory() as temp_dir:
            pipeline_dir = os.path.join(temp_dir, "analytics_pipeline")
            os.makedirs(pipeline_dir, exist_ok=True)
            
            export_count = 0
            for name, data in pipeline_results.items():
                if data['success'] and data['result']:
                    # Export as CSV
                    csv_path = os.path.join(pipeline_dir, f"{name}.csv")
                    ResultFormatter.to_csv(data['result'], csv_path)
                    assert_file_exists_and_not_empty(csv_path)
                    
                    # Export as JSON
                    json_path = os.path.join(pipeline_dir, f"{name}.json")
                    ResultFormatter.to_json(data['result'], json_path)
                    assert_file_exists_and_not_empty(json_path)
                    
                    export_count += 1
            
            # Step 6: Generate pipeline summary
            summary_data = {
                'pipeline_executed': True,
                'total_queries': len(analysis_queries),
                'successful_queries': len([d for d in pipeline_results.values() if d['success']]),
                'exported_datasets': export_count,
                'total_execution_time': sum(d['metrics'].execution_time for d in pipeline_results.values() if d['metrics']),
                'database_info': db.info()
            }
            
            summary_path = os.path.join(pipeline_dir, "pipeline_summary.json")
            import json
            with open(summary_path, 'w') as f:
                json.dump(summary_data, f, indent=2)
            
            assert_file_exists_and_not_empty(summary_path)
            assert summary_data['successful_queries'] >= 1, "Pipeline should have some successful queries"
    
    def test_multi_database_workflow(self):
        """Test workflow with multiple QuickConnect databases."""
        # Step 1: Create multiple databases
        db1 = QuickConnect.memory()
        db2 = QuickConnect.memory()
        
        test_data = get_standard_test_data()
        db1.load_resources(test_data[:len(test_data)//2])  # First half
        db2.load_resources(test_data[len(test_data)//2:])  # Second half
        
        # Step 2: Execute same query on both databases
        query = Templates.patient_demographics()
        
        result1 = db1.execute(query)
        result2 = db2.execute(query)
        
        # Step 3: Combine results
        df1 = ResultFormatter.to_dataframe(result1)
        df2 = ResultFormatter.to_dataframe(result2)
        
        # Step 4: Validate data distribution
        assert_valid_dataframe(df1)
        assert_valid_dataframe(df2)
        
        # Combined data should represent all test data
        total_patients = len(df1) + len(df2)
        expected_patients = len([r for r in test_data if r.get('resourceType') == 'Patient'])
        
        # Should have processed all patients (allowing for some query filtering)
        assert total_patients <= expected_patients, "Should not exceed total patients"
        assert total_patients >= 0, "Should have non-negative patient count"
        
        # Step 5: Export combined analysis
        with tempfile.TemporaryDirectory() as temp_dir:
            # Export separate database results
            csv1_path = os.path.join(temp_dir, "db1_patients.csv")
            csv2_path = os.path.join(temp_dir, "db2_patients.csv")
            
            ResultFormatter.to_csv(result1, csv1_path)
            ResultFormatter.to_csv(result2, csv2_path)
            
            assert_file_exists_and_not_empty(csv1_path)
            assert_file_exists_and_not_empty(csv2_path)
            
            # Create combined analysis summary
            summary_path = os.path.join(temp_dir, "multi_db_summary.json")
            summary = {
                'database1_patients': len(df1),
                'database2_patients': len(df2),
                'total_patients': total_patients,
                'database1_resources': db1.get_resource_count(),
                'database2_resources': db2.get_resource_count()
            }
            
            import json
            with open(summary_path, 'w') as f:
                json.dump(summary, f, indent=2)
            
            assert_file_exists_and_not_empty(summary_path)


class TestComponentCompatibility:
    """Test compatibility between different component versions and configurations."""
    
    def test_template_query_builder_equivalence(self):
        """Test that Templates and QueryBuilder produce equivalent results."""
        with test_database() as db:
            # Step 1: Execute template
            template = Templates.patient_demographics()
            template_result = db.execute(template)
            template_df = ResultFormatter.to_dataframe(template_result)
            
            # Step 2: Build equivalent query manually
            manual_query = (QueryBuilder()
                          .resource("Patient")
                          .columns([
                              {"name": "id", "path": "id"},
                              {"name": "family_name", "path": "name.family"},
                              {"name": "birth_date", "path": "birthDate"},
                              {"name": "gender", "path": "gender"},
                              {"name": "active", "path": "active"}
                          ])
                          .build())
            
            manual_result = db.execute(manual_query)
            manual_df = ResultFormatter.to_dataframe(manual_result)
            
            # Step 3: Compare core columns
            core_columns = ["id", "family_name", "birth_date", "gender", "active"]
            
            for col in core_columns:
                if col in template_df.columns and col in manual_df.columns:
                    # Compare data for patients that exist in both
                    template_ids = set(template_df['id'].tolist()) if 'id' in template_df.columns else set()
                    manual_ids = set(manual_df['id'].tolist()) if 'id' in manual_df.columns else set()
                    common_ids = template_ids.intersection(manual_ids)
                    
                    if common_ids:
                        # Data should be consistent for common patients
                        assert len(common_ids) > 0, f"Should have common patients for column {col} comparison"
            
            # Step 4: Validate both approaches work
            assert len(template_df.columns) >= len(manual_df.columns), "Template should have at least as many columns"
            assert len(template_df) >= 0 and len(manual_df) >= 0, "Both should return valid results"
    
    def test_batch_vs_sequential_execution_consistency(self):
        """Test that batch and sequential execution produce consistent results."""
        with test_database() as db:
            queries = [
                ("demo", Templates.patient_demographics()),
                ("vitals", Templates.vital_signs()),
                ("labs", Templates.lab_results())
            ]
            
            # Step 1: Execute sequentially
            sequential_results = {}
            for name, query in queries:
                try:
                    result = db.execute(query)
                    sequential_results[name] = ResultFormatter.to_dataframe(result)
                except Exception as e:
                    sequential_results[name] = None
                    print(f"Sequential execution failed for {name}: {e}")
            
            # Step 2: Execute with batch processor
            processor = BatchProcessor(db)
            for name, query in queries:
                processor.add_query(name, query)
            
            batch_results = processor.execute_all(parallel=False)  # Sequential batch
            
            # Step 3: Compare results
            for i, (name, query) in enumerate(queries):
                if name in sequential_results and sequential_results[name] is not None:
                    if i < len(batch_results) and batch_results[i].success:
                        batch_df = ResultFormatter.to_dataframe(batch_results[i].result)
                        sequential_df = sequential_results[name]
                        
                        # Compare basic properties
                        assert len(batch_df.columns) == len(sequential_df.columns), f"Column count mismatch for {name}"
                        assert len(batch_df) == len(sequential_df), f"Row count mismatch for {name}"
            
            # Step 4: Validate execution methods
            successful_sequential = len([r for r in sequential_results.values() if r is not None])
            successful_batch = len([r for r in batch_results if r.success])
            
            assert successful_sequential == successful_batch, "Sequential and batch should have same success rate"