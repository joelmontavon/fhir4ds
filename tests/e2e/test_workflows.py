"""
End-to-End Workflow Tests

Complete workflow tests that demonstrate real-world usage scenarios
combining multiple FHIR4DS Phase 2 components together.

These tests validate:
- Complete data analysis workflows
- Multi-step processing pipelines
- Performance monitoring integration
- Export and reporting capabilities
- Error handling and recovery
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
        ResultFormatter, BatchProcessor, PERFORMANCE_AVAILABLE
    )
    from tests.helpers.database_fixtures import test_database
    from tests.helpers.test_data_generator import get_standard_test_data, get_large_test_data
    from tests.helpers.assertion_helpers import (
        assert_valid_query_result,
        assert_valid_dataframe,
        assert_valid_json_export,
        assert_batch_results_valid,
        assert_file_exists_and_not_empty
    )
    
    # Try to import performance monitoring if available
    if PERFORMANCE_AVAILABLE:
        from fhir4ds.helpers import PerformanceMonitor
        from tests.helpers.assertion_helpers import assert_performance_metrics_valid
    else:
        PerformanceMonitor = None
        def assert_performance_metrics_valid(metrics):
            pass  # No-op if performance monitoring not available
            
except ImportError as e:
    pytest.skip(f"FHIR4DS modules not available: {e}", allow_module_level=True)


class TestBasicAnalyticsWorkflow:
    """Test basic healthcare analytics workflow."""
    
    def test_patient_demographics_analysis_workflow(self):
        """Test complete patient demographics analysis workflow."""
        with test_database() as db:
            # Step 1: Get patient demographics using template
            demographics_query = Templates.patient_demographics()
            
            # Step 2: Execute query with optional performance monitoring
            if PerformanceMonitor:
                monitor = PerformanceMonitor(db)
                result = monitor.execute_with_profiling(demographics_query)
                metrics = monitor.get_last_metrics()
            else:
                result = db.execute(demographics_query)
                metrics = None
            
            # Step 3: Validate results
            assert_valid_query_result(result)
            
            # Step 4: Convert to DataFrame for analysis
            df = ResultFormatter.to_dataframe(result)
            assert_valid_dataframe(df, expected_min_rows=0)
            
            # Step 5: Export results to different formats
            with tempfile.TemporaryDirectory() as temp_dir:
                # Export to CSV
                csv_path = os.path.join(temp_dir, "demographics.csv")
                ResultFormatter.to_csv(result, csv_path)
                assert_file_exists_and_not_empty(csv_path)
                
                # Export to JSON
                json_path = os.path.join(temp_dir, "demographics.json")
                ResultFormatter.to_json(result, json_path)
                assert_file_exists_and_not_empty(json_path)
                
                # Export to Excel
                excel_path = os.path.join(temp_dir, "demographics.xlsx")
                ResultFormatter.to_excel([result], excel_path)
                assert_file_exists_and_not_empty(excel_path)
            
            # Step 6: Validate performance metrics (if available)
            if metrics:
                assert_performance_metrics_valid(metrics)
                assert metrics.success is True
    
    def test_clinical_data_analysis_workflow(self):
        """Test clinical data analysis workflow with multiple queries."""
        with test_database() as db:
            # Step 1: Get vital signs data
            vital_signs_query = Templates.vital_signs()
            vital_signs_result = db.execute(vital_signs_query)
            
            # Step 2: Get lab results data
            lab_results_query = Templates.lab_results()
            lab_results_result = db.execute(lab_results_query)
            
            # Step 3: Get medication data
            medications_query = Templates.medications_current()
            medications_result = db.execute(medications_query)
            
            # Step 4: Process all results
            results_data = {
                'vital_signs': ResultFormatter.to_dataframe(vital_signs_result),
                'lab_results': ResultFormatter.to_dataframe(lab_results_result),
                'medications': ResultFormatter.to_dataframe(medications_result)
            }
            
            # Step 5: Validate all data
            for name, df in results_data.items():
                assert_valid_dataframe(df, expected_min_rows=0)
                assert len(df.columns) > 0, f"{name} should have columns"
            
            # Step 6: Validate all queries executed successfully
            for name, result in [('vital_signs', vital_signs_result), ('lab_results', lab_results_result), ('medications', medications_result)]:
                assert_valid_query_result(result)


class TestQueryBuilderWorkflow:
    """Test workflows using QueryBuilder for custom queries."""
    
    def test_custom_patient_query_workflow(self):
        """Test building and executing custom patient queries."""
        with test_database() as db:
            # Step 1: Build custom query using QueryBuilder
            custom_query = (QueryBuilder()
                          .resource("Patient")
                          .columns([
                              {"name": "patient_id", "path": "id", "type": "id"},
                              {"name": "family_name", "path": "name.family", "type": "string"},
                              {"name": "birth_date", "path": "birthDate", "type": "date"},
                              {"name": "gender", "path": "gender", "type": "code"},
                              {"name": "active_status", "path": "active", "type": "boolean"}
                          ])
                          .where("active = true")
                          .where("gender = 'male'")
                          .build())
            
            # Step 2: Execute query
            result = db.execute(custom_query)
            assert_valid_query_result(result)
            
            # Step 3: Process results
            df = ResultFormatter.to_dataframe(result)
            
            # Step 4: Validate filtering worked
            assert_valid_dataframe(df)
            if len(df) > 0:
                # Check that all returned patients are male and active
                assert all(df['gender'] == 'male'), "All patients should be male"
                assert all(df['active_status'] == True), "All patients should be active"
            
            # Step 5: Export filtered results
            with tempfile.TemporaryDirectory() as temp_dir:
                export_path = os.path.join(temp_dir, "male_patients.json")
                json_data = ResultFormatter.to_json(result, export_path)
                assert_file_exists_and_not_empty(export_path)
    
    def test_observation_analysis_workflow(self):
        """Test custom observation analysis workflow."""
        with test_database() as db:
            monitor = PerformanceMonitor(db)
            
            # Step 1: Build custom observation query
            obs_query = (QueryBuilder()
                        .resource("Observation")
                        .columns([
                            {"name": "obs_id", "path": "id"},
                            {"name": "patient_ref", "path": "subject.reference"},
                            {"name": "code", "path": "code.coding.code"},
                            {"name": "display", "path": "code.coding.display"},
                            {"name": "value", "path": "valueQuantity.value"},
                            {"name": "unit", "path": "valueQuantity.unit"},
                            {"name": "date", "path": "effectiveDateTime"}
                        ])
                        .where("status = 'final'")
                        .where("valueQuantity.value IS NOT NULL")
                        .build())
            
            # Step 2: Execute and monitor
            result = monitor.execute_with_profiling(obs_query)
            assert_valid_query_result(result)
            
            # Step 3: Analyze results
            df = ResultFormatter.to_dataframe(result)
            assert_valid_dataframe(df)
            
            # Step 4: Check data quality
            if len(df) > 0:
                # Should have numeric values
                assert 'value' in df.columns
                # Should have valid codes
                assert 'code' in df.columns
            
            # Step 5: Get performance insights
            metrics = monitor.get_last_metrics()
            assert_performance_metrics_valid(metrics)


class TestBatchProcessingWorkflow:
    """Test batch processing workflows."""
    
    def test_multiple_template_batch_execution(self):
        """Test executing multiple templates in batch."""
        with test_database() as db:
            # Step 1: Prepare multiple queries
            queries = [
                ("patient_demographics", Templates.patient_demographics()),
                ("vital_signs", Templates.vital_signs()),
                ("lab_results", Templates.lab_results()),
                ("medications", Templates.medications_current()),
                ("encounters", Templates.encounters_summary())
            ]
            
            # Step 2: Execute batch processing
            processor = BatchProcessor(db)
            
            # Add all queries to batch
            for name, query in queries:
                processor.add_query(name, query)
            
            # Step 3: Execute batch with monitoring
            batch_results = processor.execute_all(parallel=True, monitor_performance=True)
            
            # Step 4: Validate batch results
            assert_batch_results_valid(batch_results, expected_count=len(queries))
            
            # Step 5: Check individual results
            for i, (name, query) in enumerate(queries):
                result = batch_results[i]
                assert result.success is True, f"{name} query should succeed"
                assert result.execution_time > 0, f"{name} should have execution time"
                assert result.result is not None, f"{name} should have result data"
            
            # Step 6: Export batch results
            with tempfile.TemporaryDirectory() as temp_dir:
                batch_export_dir = os.path.join(temp_dir, "batch_results")
                os.makedirs(batch_export_dir, exist_ok=True)
                
                for i, (name, query) in enumerate(queries):
                    if batch_results[i].success and batch_results[i].result:
                        export_path = os.path.join(batch_export_dir, f"{name}.csv")
                        ResultFormatter.to_csv(batch_results[i].result, export_path)
                        assert_file_exists_and_not_empty(export_path)
    
    def test_custom_query_batch_workflow(self):
        """Test batch processing with custom QueryBuilder queries."""
        with test_database() as db:
            # Step 1: Build multiple custom queries
            queries = [
                ("active_patients", 
                 FHIRQueryBuilder.patient_demographics()
                 .where("active = true")
                 .build()),
                
                ("recent_observations",
                 FHIRQueryBuilder.observation_values()
                 .where("effectiveDateTime >= '2023-01-01'")
                 .build()),
                
                ("current_medications",
                 FHIRQueryBuilder.medication_list()
                 .where("status = 'active'")
                 .build())
            ]
            
            # Step 2: Execute batch
            processor = BatchProcessor(db)
            for name, query in queries:
                processor.add_query(name, query)
            
            batch_results = processor.execute_all(
                parallel=True, 
                monitor_performance=True,
                max_workers=2
            )
            
            # Step 3: Validate results
            assert_batch_results_valid(batch_results, expected_count=len(queries))
            
            # Step 4: Analyze performance
            total_time = sum(result.execution_time for result in batch_results)
            successful_count = sum(1 for result in batch_results if result.success)
            
            assert successful_count >= len(queries) // 2, "At least half of queries should succeed"
            assert total_time < 30.0, "Total batch execution should be under 30 seconds"


class TestComplexAnalyticsWorkflow:
    """Test complex multi-step analytics workflows."""
    
    def test_patient_cohort_analysis_workflow(self):
        """Test complete patient cohort analysis workflow."""
        with test_database() as db:
            monitor = PerformanceMonitor(db)
            
            # Step 1: Identify patient cohort
            cohort_query = Templates.cohort_identification(["E11", "E10"])  # Diabetes codes
            cohort_result = monitor.execute_with_profiling(cohort_query)
            
            # Step 2: Get demographics for cohort
            demographics_query = Templates.patient_demographics()
            demographics_result = monitor.execute_with_profiling(demographics_query)
            
            # Step 3: Get clinical data for cohort
            clinical_queries = [
                ("observations", Templates.vital_signs()),
                ("lab_results", Templates.lab_results()),
                ("medications", Templates.medications_current())
            ]
            
            clinical_results = {}
            for name, query in clinical_queries:
                result = monitor.execute_with_profiling(query)
                clinical_results[name] = result
            
            # Step 4: Combine and analyze data
            cohort_df = ResultFormatter.to_dataframe(cohort_result) if cohort_result else None
            demographics_df = ResultFormatter.to_dataframe(demographics_result)
            
            # Validate core data
            assert_valid_dataframe(demographics_df)
            
            # Step 5: Generate comprehensive report
            with tempfile.TemporaryDirectory() as temp_dir:
                report_dir = os.path.join(temp_dir, "cohort_analysis")
                os.makedirs(report_dir, exist_ok=True)
                
                # Export demographics
                demographics_path = os.path.join(report_dir, "demographics.xlsx")
                ResultFormatter.to_excel([demographics_result], demographics_path)
                assert_file_exists_and_not_empty(demographics_path)
                
                # Export clinical data
                for name, result in clinical_results.items():
                    if result:
                        clinical_path = os.path.join(report_dir, f"{name}.csv")
                        ResultFormatter.to_csv(result, clinical_path)
                        assert_file_exists_and_not_empty(clinical_path)
                
                # Export performance report
                performance_metrics = monitor.get_all_metrics()
                performance_path = os.path.join(report_dir, "performance_report.json")
                with open(performance_path, 'w') as f:
                    metrics_data = []
                    for metrics in performance_metrics:
                        metrics_data.append({
                            'execution_time': metrics.execution_time,
                            'success': metrics.success,
                            'row_count': metrics.row_count,
                            'efficiency_score': metrics.efficiency_score
                        })
                    json.dump(metrics_data, f, indent=2)
                assert_file_exists_and_not_empty(performance_path)
    
    def test_dashboard_data_preparation_workflow(self):
        """Test preparing data for dashboard visualization."""
        with test_database() as db:
            monitor = PerformanceMonitor(db)
            
            # Step 1: Gather all dashboard data sources
            dashboard_queries = {
                'patient_summary': Templates.patient_demographics(),
                'vital_trends': Templates.vital_signs(),
                'lab_trends': Templates.lab_results(),
                'medication_usage': Templates.medications_current(),
                'encounter_summary': Templates.encounters_summary()
            }
            
            # Step 2: Execute all queries with batch processing
            processor = BatchProcessor(db)
            for name, query in dashboard_queries.items():
                processor.add_query(name, query)
            
            batch_results = processor.execute_all(
                parallel=True,
                monitor_performance=True,
                max_workers=3
            )
            
            # Step 3: Validate all data sources
            assert_batch_results_valid(batch_results, expected_count=len(dashboard_queries))
            
            # Step 4: Prepare dashboard exports
            with tempfile.TemporaryDirectory() as temp_dir:
                dashboard_dir = os.path.join(temp_dir, "dashboard_data")
                os.makedirs(dashboard_dir, exist_ok=True)
                
                successful_exports = 0
                for i, (name, query) in enumerate(dashboard_queries.items()):
                    result = batch_results[i]
                    if result.success and result.result:
                        # Export as JSON for web dashboard
                        json_path = os.path.join(dashboard_dir, f"{name}.json")
                        ResultFormatter.to_json(result.result, json_path)
                        assert_file_exists_and_not_empty(json_path)
                        
                        # Export as CSV for analysis
                        csv_path = os.path.join(dashboard_dir, f"{name}.csv")
                        ResultFormatter.to_csv(result.result, csv_path)
                        assert_file_exists_and_not_empty(csv_path)
                        
                        successful_exports += 1
                
                # Should export most data sources successfully
                assert successful_exports >= len(dashboard_queries) // 2
                
                # Step 5: Create dashboard manifest
                manifest_path = os.path.join(dashboard_dir, "manifest.json")
                manifest_data = {
                    'created_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'data_sources': list(dashboard_queries.keys()),
                    'successful_exports': successful_exports,
                    'total_queries': len(dashboard_queries)
                }
                with open(manifest_path, 'w') as f:
                    json.dump(manifest_data, f, indent=2)
                assert_file_exists_and_not_empty(manifest_path)


class TestErrorHandlingWorkflow:
    """Test error handling in complete workflows."""
    
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
            
            # Step 2: Execute with batch processor
            processor = BatchProcessor(db)
            for name, query in queries:
                processor.add_query(name, query)
            
            batch_results = processor.execute_all(
                parallel=False,  # Sequential to test error isolation
                monitor_performance=True,
                continue_on_error=True
            )
            
            # Step 3: Validate error handling
            assert len(batch_results) == len(queries)
            
            # Should have some successes and some failures
            success_count = sum(1 for result in batch_results if result.success)
            failure_count = sum(1 for result in batch_results if not result.success)
            
            assert success_count >= 1, "Should have at least one successful query"
            assert failure_count >= 1, "Should have at least one failed query"
            
            # Step 4: Process only successful results
            with tempfile.TemporaryDirectory() as temp_dir:
                success_dir = os.path.join(temp_dir, "successful_results")
                os.makedirs(success_dir, exist_ok=True)
                
                exported_count = 0
                for i, (name, query) in enumerate(queries):
                    result = batch_results[i]
                    if result.success and result.result:
                        export_path = os.path.join(success_dir, f"{name}.json")
                        ResultFormatter.to_json(result.result, export_path)
                        assert_file_exists_and_not_empty(export_path)
                        exported_count += 1
                
                assert exported_count == success_count, "Should export all successful results"
    
    def test_performance_degradation_handling(self):
        """Test handling of performance issues in workflows."""
        with test_database() as db:
            monitor = PerformanceMonitor(db)
            
            # Step 1: Execute queries and monitor performance
            queries_to_test = [
                Templates.patient_demographics(),
                Templates.vital_signs(),
                Templates.lab_results()
            ]
            
            all_metrics = []
            for i, query in enumerate(queries_to_test):
                result = monitor.execute_with_profiling(query)
                metrics = monitor.get_last_metrics()
                all_metrics.append(metrics)
                
                # Validate each query execution
                assert_valid_query_result(result)
                assert_performance_metrics_valid(metrics)
            
            # Step 2: Analyze performance trends
            execution_times = [m.execution_time for m in all_metrics]
            efficiency_scores = [m.efficiency_score for m in all_metrics]
            
            # All queries should complete in reasonable time
            max_time = max(execution_times)
            assert max_time < 10.0, f"Maximum execution time too high: {max_time:.3f}s"
            
            # Efficiency scores should be reasonable
            min_efficiency = min(efficiency_scores)
            assert min_efficiency > 20.0, f"Minimum efficiency too low: {min_efficiency:.1f}"
            
            # Step 3: Generate performance report
            with tempfile.TemporaryDirectory() as temp_dir:
                report_path = os.path.join(temp_dir, "performance_analysis.json")
                
                performance_report = {
                    'total_queries': len(queries_to_test),
                    'total_time': sum(execution_times),
                    'average_time': sum(execution_times) / len(execution_times),
                    'max_time': max_time,
                    'min_time': min(execution_times),
                    'average_efficiency': sum(efficiency_scores) / len(efficiency_scores),
                    'min_efficiency': min_efficiency,
                    'max_efficiency': max(efficiency_scores),
                    'all_successful': all(m.success for m in all_metrics)
                }
                
                with open(report_path, 'w') as f:
                    json.dump(performance_report, f, indent=2)
                
                assert_file_exists_and_not_empty(report_path)
                assert performance_report['all_successful'] is True


class TestRealWorldScenarios:
    """Test real-world healthcare analytics scenarios."""
    
    def test_monthly_reporting_workflow(self):
        """Test monthly healthcare reporting workflow."""
        with test_database() as db:
            # Step 1: Generate monthly report data
            monthly_queries = {
                'patient_registrations': Templates.patient_demographics(),
                'clinical_activities': Templates.encounters_summary(),
                'medication_dispensing': Templates.medications_current(),
                'lab_activities': Templates.lab_results(),
                'vital_sign_monitoring': Templates.vital_signs()
            }
            
            # Step 2: Execute all monthly queries
            processor = BatchProcessor(db)
            for name, query in monthly_queries.items():
                processor.add_query(name, query)
            
            start_time = time.time()
            batch_results = processor.execute_all(
                parallel=True,
                monitor_performance=True,
                max_workers=3
            )
            total_time = time.time() - start_time
            
            # Step 3: Validate monthly data processing
            assert_batch_results_valid(batch_results, expected_count=len(monthly_queries))
            assert total_time < 60.0, "Monthly report generation should complete within 1 minute"
            
            # Step 4: Generate comprehensive monthly report
            with tempfile.TemporaryDirectory() as temp_dir:
                monthly_dir = os.path.join(temp_dir, "monthly_report")
                os.makedirs(monthly_dir, exist_ok=True)
                
                # Export each dataset
                for i, (name, query) in enumerate(monthly_queries.items()):
                    result = batch_results[i]
                    if result.success and result.result:
                        # Excel for stakeholders
                        excel_path = os.path.join(monthly_dir, f"{name}.xlsx")
                        ResultFormatter.to_excel([result.result], excel_path)
                        assert_file_exists_and_not_empty(excel_path)
                        
                        # CSV for analysis
                        csv_path = os.path.join(monthly_dir, f"{name}.csv")
                        ResultFormatter.to_csv(result.result, csv_path)
                        assert_file_exists_and_not_empty(csv_path)
                
                # Generate summary report
                summary_path = os.path.join(monthly_dir, "report_summary.json")
                summary_data = {
                    'report_date': time.strftime('%Y-%m-%d'),
                    'processing_time_seconds': total_time,
                    'datasets_generated': len([r for r in batch_results if r.success]),
                    'total_datasets_requested': len(monthly_queries),
                    'datasets': {}
                }
                
                for i, (name, query) in enumerate(monthly_queries.items()):
                    result = batch_results[i]
                    summary_data['datasets'][name] = {
                        'success': result.success,
                        'execution_time': result.execution_time,
                        'row_count': len(ResultFormatter.to_dataframe(result.result)) if result.success and result.result else 0
                    }
                
                with open(summary_path, 'w') as f:
                    json.dump(summary_data, f, indent=2)
                assert_file_exists_and_not_empty(summary_path)
    
    def test_research_data_extraction_workflow(self):
        """Test research data extraction workflow."""
        with test_database() as db:
            monitor = PerformanceMonitor(db)
            
            # Step 1: Define research cohort
            research_queries = [
                ("cohort_patients", Templates.cohort_identification(["E11", "E10", "I10"])),  # Diabetes + Hypertension
                ("baseline_demographics", Templates.patient_demographics()),
                ("clinical_observations", Templates.observations_by_code("85354-9")),  # Blood pressure
                ("lab_values", Templates.diabetes_a1c_monitoring())
            ]
            
            # Step 2: Extract research data with detailed monitoring
            research_data = {}
            for name, query in research_queries:
                result = monitor.execute_with_profiling(query)
                research_data[name] = {
                    'result': result,
                    'metrics': monitor.get_last_metrics()
                }
            
            # Step 3: Validate research data quality
            for name, data in research_data.items():
                if data['result']:
                    assert_valid_query_result(data['result'])
                assert_performance_metrics_valid(data['metrics'])
            
            # Step 4: Export research dataset
            with tempfile.TemporaryDirectory() as temp_dir:
                research_dir = os.path.join(temp_dir, "research_export")
                os.makedirs(research_dir, exist_ok=True)
                
                # Export all datasets
                for name, data in research_data.items():
                    if data['result'] and data['metrics'].success:
                        # CSV for statistical analysis
                        csv_path = os.path.join(research_dir, f"{name}.csv")
                        ResultFormatter.to_csv(data['result'], csv_path)
                        assert_file_exists_and_not_empty(csv_path)
                
                # Create research metadata
                metadata_path = os.path.join(research_dir, "research_metadata.json")
                metadata = {
                    'extraction_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'cohort_definition': ['E11', 'E10', 'I10'],
                    'datasets': {},
                    'total_extraction_time': sum(data['metrics'].execution_time for data in research_data.values()),
                    'data_quality_score': sum(data['metrics'].efficiency_score for data in research_data.values()) / len(research_data)
                }
                
                for name, data in research_data.items():
                    metadata['datasets'][name] = {
                        'success': data['metrics'].success,
                        'row_count': data['metrics'].row_count,
                        'execution_time': data['metrics'].execution_time,
                        'efficiency_score': data['metrics'].efficiency_score
                    }
                
                with open(metadata_path, 'w') as f:
                    json.dump(metadata, f, indent=2)
                assert_file_exists_and_not_empty(metadata_path)
                
                # Validate research data completeness
                assert metadata['data_quality_score'] > 50.0, "Research data quality should be acceptable"
                successful_datasets = sum(1 for data in research_data.values() if data['metrics'].success)
                assert successful_datasets >= len(research_queries) // 2, "Most research queries should succeed"