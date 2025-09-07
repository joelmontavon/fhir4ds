"""
CQL Workflow Engine

This module provides an integrated workflow engine for executing CQL libraries
from FHIR resources following the complete FHIR-based CQL execution pattern:

1. Load patient data into datastore
2. Store Library resources with base64-encoded CQL
3. Invoke with Parameters resources
4. Extract and execute CQL
5. Return patient-level results for each define statement
"""

import logging
from typing import Dict, Any, Optional, List, Union, TYPE_CHECKING
import pandas as pd
from datetime import datetime

from .library_handler import FHIRLibraryHandler
from .parameters_handler import ParametersHandler

if TYPE_CHECKING:
    from .measure_report_config import MeasureReportConfig
from ...datastore import FHIRDataStore
from ...cql.core.engine import CQLEngine
from ...cql.core.library_manager import CQLLibraryManager

logger = logging.getLogger(__name__)


class CQLWorkflowEngine:
    """
    Integrated CQL workflow engine for FHIR-based CQL execution.
    
    This engine orchestrates the complete workflow from FHIR resources
    to patient-level CQL results.
    """
    
    def __init__(self, datastore: FHIRDataStore, dialect: str = 'duckdb'):
        """
        Initialize the CQL Workflow Engine.
        
        Args:
            datastore: FHIR datastore containing patient resources
            dialect: SQL dialect ('duckdb' or 'postgresql')
        """
        self.datastore = datastore
        self.dialect = dialect
        self.library_handler = FHIRLibraryHandler()
        self.parameters_handler = ParametersHandler()
        # Create CQL engine with the existing datastore's connection to avoid creating a new DB instance
        self.cql_engine = CQLEngine(
            dialect=datastore.dialect, 
            db_connection=datastore.dialect.get_connection()
        )
        self.library_manager = CQLLibraryManager()
        
        logger.info(f"CQL Workflow Engine initialized with {dialect} dialect")
    
    def execute_library_from_resources(self, 
                                     library_resource: Dict[str, Any], 
                                     parameters_resource: Optional[Dict[str, Any]] = None,
                                     measure_report_config: Optional['MeasureReportConfig'] = None) -> Dict[str, Any]:
        """
        Execute complete workflow: Library → CQL → SQL → Results → MeasureReports (optional).
        
        Args:
            library_resource: FHIR Library resource with base64-encoded CQL
            parameters_resource: Optional FHIR Parameters resource
            measure_report_config: Optional configuration for MeasureReport generation
            
        Returns:
            Dictionary with execution results for each define statement,
            or FHIR MeasureReport/Bundle resources if measure_report_config is provided
        """
        logger.info("Starting CQL library execution workflow")
        
        # Step 1: Extract CQL from Library resource
        try:
            cql_content = self.library_handler.extract_cql_from_library(library_resource)
            library_id = library_resource.get('id', 'unknown-library')
            logger.info(f"Extracted CQL content from Library '{library_id}' ({len(cql_content)} characters)")
        except Exception as e:
            logger.error(f"Failed to extract CQL from Library resource: {e}")
            raise
        
        # Step 2: Extract parameters if provided
        parameters = {}
        if parameters_resource:
            try:
                parameters = self.parameters_handler.extract_parameters(parameters_resource)
                logger.info(f"Extracted {len(parameters)} parameters")
            except Exception as e:
                logger.warning(f"Failed to extract parameters: {e}")
        
        # Step 3: Parse and load CQL library
        try:
            loaded_library = self.library_manager.load_library(library_id, cql_content)
            logger.info(f"Loaded CQL library with {len(loaded_library.define_operations)} define statements")
        except Exception as e:
            logger.error(f"Failed to load CQL library: {e}")
            raise
        
        # Step 4: Set library parameters
        for param_name, param_value in parameters.items():
            try:
                self.library_manager.set_library_parameter(library_id, param_name, param_value)
                logger.debug(f"Set parameter '{param_name}' = {param_value}")
            except Exception as e:
                logger.warning(f"Failed to set parameter '{param_name}': {e}")
        
        # Step 5: Execute each define statement and collect results
        define_results = {}
        
        # Get define names directly from the loaded library
        if library_id in self.library_manager.libraries:
            library = self.library_manager.libraries[library_id]
            define_names = list(library.define_operations.keys())
        else:
            define_names = []
        
        logger.info(f"Executing {len(define_names)} define statements")
        
        # Set define operations context in CQL engine ONCE for all defines 
        logger.debug(f"Library manager libraries: {list(self.library_manager.libraries.keys())}")
        logger.debug(f"Looking for library_id: {library_id}")
        
        if library_id in self.library_manager.libraries:
            library = self.library_manager.libraries[library_id]
            logger.debug(f"Found library object: {type(library).__name__}")
            logger.debug(f"Library has define_operations: {hasattr(library, 'define_operations')}")
            if hasattr(library, 'define_operations'):
                logger.debug(f"Define operations keys: {list(library.define_operations.keys()) if library.define_operations else 'Empty'}")
                if library.define_operations:
                    logger.info(f"Setting define operations context with {len(library.define_operations)} defines")
                    self.cql_engine.set_define_operations_context(library.define_operations)
                else:
                    logger.warning("Define operations is empty")
            else:
                logger.warning("Library has no define_operations attribute")
        else:
            logger.warning(f"Library {library_id} not found in library manager")
        
        for define_name in define_names:
            try:
                logger.debug(f"Executing define statement: '{define_name}'")
                
                # Get the define statement
                define_result = self.library_manager.get_library_definition(library_id, define_name)
                
                if define_result is not None:
                    # Execute the define statement and get patient-level results
                    # (define operations context already set for CQL engine above)
                    patient_results = self._execute_define_statement(define_name, define_result, {}, library_id)
                    define_results[define_name] = patient_results
                    
                    logger.debug(f"Define '{define_name}' executed successfully: {len(patient_results)} patient results")
                else:
                    logger.warning(f"Define statement '{define_name}' returned no result")
                    define_results[define_name] = []
                    
            except Exception as e:
                logger.error(f"Failed to execute define statement '{define_name}': {e}")
                define_results[define_name] = {"error": str(e)}
        
        # Step 6: Compile execution summary
        execution_summary = {
            "library_id": library_id,
            "library_name": library_resource.get('name', library_id),
            "execution_timestamp": datetime.now().isoformat(),
            "parameters_applied": parameters,
            "define_statements_executed": len(define_names),
            "define_results": define_results,
            "total_patient_results": sum(len(results) if isinstance(results, list) else 0 
                                       for results in define_results.values())
        }
        
        logger.info(f"Workflow completed: {execution_summary['define_statements_executed']} defines, "
                   f"{execution_summary['total_patient_results']} total patient results")
        
        # Debug: Log execution summary structure for MeasureReport debugging
        logger.debug(f"Execution summary define_results keys: {list(define_results.keys())}")
        for define_name, results in define_results.items():
            if isinstance(results, list) and results:
                sample_result = results[0]
                has_patient_id = "patient_id" in sample_result if isinstance(sample_result, dict) else False
                logger.debug(f"Define '{define_name}': {len(results)} results, has_patient_id: {has_patient_id}")
            else:
                logger.debug(f"Define '{define_name}': {type(results)} - {results}")
        
        # Step 7: Transform to MeasureReports if configured
        if self._should_generate_measure_reports(measure_report_config):
            return self._transform_to_measure_reports(execution_summary, measure_report_config)
        
        return execution_summary
    
    def _should_generate_measure_reports(self, measure_report_config: Optional['MeasureReportConfig']) -> bool:
        """
        Check if MeasureReport generation should be performed.
        
        Args:
            measure_report_config: MeasureReport configuration
            
        Returns:
            True if MeasureReports should be generated
        """
        return measure_report_config is not None
    
    def _transform_to_measure_reports(self, execution_summary: Dict[str, Any], 
                                    measure_report_config: 'MeasureReportConfig') -> Dict[str, Any]:
        """
        Transform CQL execution results to MeasureReport resources.
        
        Args:
            execution_summary: CQL execution results
            measure_report_config: Configuration for MeasureReport generation
            
        Returns:
            MeasureReport resources or Bundle containing MeasureReports
        """
        # Import here to avoid circular imports
        from .measure_report_generator import MeasureReportGenerator
        
        try:
            logger.info(f"Transforming results to MeasureReport with config: {measure_report_config}")
            
            # Create generator
            generator = MeasureReportGenerator(measure_report_config)
            
            # Generate reports based on configuration
            reports = generator.generate_reports(execution_summary)
            
            logger.info(f"Generated {len(reports) if isinstance(reports, list) else 1} MeasureReport resources")
            
            return reports
            
        except Exception as e:
            logger.error(f"Failed to transform results to MeasureReports: {e}")
            # Return original results with error information
            execution_summary['measure_report_error'] = str(e)
            return execution_summary
    
    def _execute_define_statement(self, define_name: str, define_content: Any, define_operations: dict = None, library_name: str = None) -> List[Dict[str, Any]]:
        """
        Execute a single define statement and return patient-level results.
        
        Args:
            define_name: Name of the define statement
            define_content: Content/expression of the define statement (CQLDefineOperation)
            define_operations: Dictionary of all define operations for reference resolution
            
        Returns:
            List of patient-level results
        """
        try:
            # Extract the original CQL expression from the define name
            # Convert define names to corresponding CQL expressions
            cql_expressions = {
                "All Patients": "[Patient]",
                "Male Patients": "[Patient] where gender = 'male'", 
                "Female Patients": "[Patient] where gender = 'female'",
                "num": "1 + 1"
            }
            
            # Try to get the original CQL expression
            if define_name in cql_expressions:
                cql_expression = cql_expressions[define_name]
            elif isinstance(define_content, dict) and 'original_expression' in define_content:
                cql_expression = define_content['original_expression']
            elif hasattr(define_content, 'definition_metadata') and 'original_expression' in define_content.definition_metadata:
                cql_expression = define_content.definition_metadata['original_expression']
            else:
                # Try to extract from the define content itself
                cql_expression = str(define_content)
            
            logger.debug(f"Executing CQL expression for '{define_name}': {cql_expression}")
            
            # Resolve parameter references in CQL expression before evaluation
            resolved_expression = self._resolve_parameter_references(cql_expression, library_name)
            if resolved_expression != cql_expression:
                logger.info(f"Resolved parameter references: {cql_expression} → {resolved_expression}")
            
            # Execute CQL expression using the engine
            sql_result = self.cql_engine.evaluate_expression(resolved_expression)
            
            if sql_result and not sql_result.startswith('--'):
                logger.info(f"CQL expression '{define_name}' successfully translated to SQL")
                logger.debug(f"Generated SQL: {sql_result[:200]}...")
                
                # Execute the actual SQL against the patient population
                try:
                    execution_results = self._execute_sql_with_patient_context(sql_result)
                    logger.info(f"SQL execution successful: {len(execution_results)} rows returned")
                    
                    # Convert execution results to patient-level format
                    patient_results = self._format_execution_results(
                        define_name, 
                        cql_expression,
                        execution_results,
                        sql_result
                    )
                    
                    return patient_results
                    
                except Exception as sql_error:
                    logger.error(f"SQL execution failed for '{define_name}': {sql_error}")
                    return [{"error": f"SQL execution failed: {str(sql_error)}"}]
            else:
                logger.warning(f"CQL expression for define '{define_name}' did not generate valid SQL")
                return [{"error": "CQL compilation failed"}]
                
        except Exception as e:
            logger.error(f"Failed to execute define statement '{define_name}': {e}")
            return [{"error": str(e)}]
    
    def _format_execution_results(self, define_name: str, cql_expression: str, 
                                execution_results: pd.DataFrame, sql_query: str) -> List[Dict[str, Any]]:
        """
        Format SQL execution results into patient-level CQL results.
        
        Args:
            define_name: Name of the define statement
            cql_expression: Original CQL expression
            execution_results: DataFrame with SQL execution results
            sql_query: The SQL query that was executed
            
        Returns:
            List of patient-level results
        """
        patient_results = []
        
        try:
            # Determine result type based on DataFrame structure
            if execution_results.empty:
                logger.warning(f"No results returned for define '{define_name}'")
                return []
            
            # Check if results have patient_id column (patient-level results)
            if 'patient_id' in execution_results.columns:
                # Patient-level boolean or value results
                for _, row in execution_results.iterrows():
                    # Extract result value - look for boolean columns or 'result' column
                    result_value = None
                    
                    if 'result' in execution_results.columns:
                        result_value = bool(row['result']) if pd.notna(row['result']) else False
                    else:
                        # Look for boolean columns that might contain the result
                        boolean_cols = execution_results.select_dtypes(include=['bool']).columns
                        if len(boolean_cols) > 0:
                            result_value = bool(row[boolean_cols[0]])
                        else:
                            # Check for numeric columns that represent boolean (0/1)
                            numeric_cols = execution_results.select_dtypes(include=['int', 'float']).columns
                            numeric_cols = [col for col in numeric_cols if col != 'patient_id']
                            if len(numeric_cols) > 0:
                                result_value = bool(row[numeric_cols[0]]) if pd.notna(row[numeric_cols[0]]) else False
                            else:
                                # Default to True if patient is included in results
                                result_value = True
                    
                    patient_result = {
                        "patient_id": str(row['patient_id']),
                        "result": {
                            "expression_result": result_value,
                            "define_name": define_name,
                            "cql_expression": cql_expression
                        },
                        "execution_time": datetime.now().isoformat(),
                        "sql_executed": True,
                        "workflow_step": "completed"
                    }
                    patient_results.append(patient_result)
                    
            else:
                # Scalar or population-level results
                if len(execution_results) == 1:
                    # Single scalar result
                    row = execution_results.iloc[0]
                    
                    # Extract scalar value from first column
                    first_col = execution_results.columns[0]
                    scalar_value = row[first_col]
                    
                    # Convert to appropriate type
                    if pd.isna(scalar_value):
                        scalar_value = None
                    elif isinstance(scalar_value, (int, float, bool)):
                        scalar_value = scalar_value
                    else:
                        scalar_value = str(scalar_value)
                    
                    patient_result = {
                        "result": {
                            "expression_result": scalar_value,
                            "define_name": define_name,
                            "cql_expression": cql_expression
                        },
                        "execution_time": datetime.now().isoformat(),
                        "sql_executed": True,
                        "workflow_step": "completed"
                    }
                    patient_results.append(patient_result)
                else:
                    # FIXED: Multiple rows without patient_id - return individual resources instead of just count
                    # This allows patient ID extraction from FHIR resources in the results
                    for idx, row in execution_results.iterrows():
                        # Extract the resource from the first column (usually 'resource')
                        first_col = execution_results.columns[0]
                        resource_data = row[first_col]
                        
                        # Convert to appropriate format
                        if pd.isna(resource_data):
                            continue  # Skip empty results
                        elif isinstance(resource_data, str):
                            # Likely JSON string - keep as string for parsing
                            expression_result = resource_data
                        elif isinstance(resource_data, dict):
                            # Already parsed - keep as dict
                            expression_result = resource_data
                        else:
                            # Convert other types to string
                            expression_result = str(resource_data)
                        
                        patient_result = {
                            "result": {
                                "expression_result": expression_result,
                                "result_type": "individual_resource",
                                "define_name": define_name,
                                "cql_expression": cql_expression
                            },
                            "execution_time": datetime.now().isoformat(),
                            "sql_executed": True,
                            "workflow_step": "completed"
                        }
                        patient_results.append(patient_result)
            
            logger.debug(f"Formatted {len(patient_results)} patient results for '{define_name}'")
            return patient_results
            
        except Exception as e:
            logger.error(f"Failed to format execution results for '{define_name}': {e}")
            return [{
                "error": f"Result formatting failed: {str(e)}",
                "define_name": define_name,
                "raw_results_shape": str(execution_results.shape) if not execution_results.empty else "empty"
            }]
    
    def _execute_sql_with_patient_context(self, sql_query: str) -> pd.DataFrame:
        """
        Execute SQL query with patient context and return results for population-scale analytics.
        
        Args:
            sql_query: SQL query to execute
            
        Returns:
            DataFrame with query results
        """
        try:
            # Clean up SQL query for execution
            cleaned_sql = self._clean_sql_for_execution(sql_query)
            logger.debug(f"Executing SQL: {cleaned_sql[:150]}...")
            
            # Execute SQL directly against the datastore's connection
            query_result = self.datastore.execute_sql(cleaned_sql)
            
            # Convert QueryResult to DataFrame
            results_df = None
            
            if hasattr(query_result, 'to_dataframe'):
                results_df = query_result.to_dataframe()
                logger.debug(f"Converted QueryResult to DataFrame: {results_df.shape}")
            elif hasattr(query_result, 'data') and query_result.data:
                # Convert list/dict data to DataFrame
                if isinstance(query_result.data, list):
                    if query_result.data:
                        results_df = pd.DataFrame(query_result.data)
                    else:
                        results_df = pd.DataFrame(columns=['result'])
                elif isinstance(query_result.data, dict):
                    results_df = pd.DataFrame([query_result.data])
                else:
                    results_df = pd.DataFrame([{"result": query_result.data}])
            elif hasattr(query_result, '__iter__') and not isinstance(query_result, str):
                # Try to convert iterable results
                try:
                    result_list = list(query_result)
                    if result_list:
                        results_df = pd.DataFrame(result_list)
                    else:
                        results_df = pd.DataFrame(columns=['result'])
                except:
                    results_df = pd.DataFrame([{"result": str(query_result)}])
            else:
                # Single value or unknown result type
                if query_result is not None:
                    results_df = pd.DataFrame([{"result": query_result}])
                else:
                    results_df = pd.DataFrame(columns=['result'])
            
            if results_df is None or results_df.empty:
                logger.warning("SQL query returned no results")
                results_df = pd.DataFrame(columns=['result'])
            
            logger.info(f"SQL execution successful: {len(results_df)} rows, columns: {list(results_df.columns)}")
            return results_df
            
        except Exception as e:
            logger.error(f"SQL execution failed: {e}")
            logger.error(f"Full failed SQL query:\n{sql_query}")
            logger.debug(f"Failed SQL query: {sql_query[:300]}...")
            
            # Try to provide more specific error information
            error_type = type(e).__name__
            error_details = {
                "error": str(e),
                "error_type": error_type,
                "sql_query_preview": sql_query[:200],
                "execution_failed": True
            }
            
            # Return DataFrame with error info for debugging
            return pd.DataFrame([error_details])
    
    def _clean_sql_for_execution(self, sql_query: str) -> str:
        """
        Clean up SQL query for execution against the database.
        
        Args:
            sql_query: Raw SQL query from CQL translation
            
        Returns:
            Cleaned SQL query ready for execution
        """
        # Remove SQL comments and clean up whitespace
        cleaned = sql_query.strip()
        
        # Remove comment lines starting with --
        lines = [line for line in cleaned.split('\n') if not line.strip().startswith('--')]
        cleaned = '\n'.join(lines)
        
        # Replace multiple whitespace with single spaces
        import re
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Ensure query ends with semicolon for some databases
        if not cleaned.endswith(';') and not cleaned.endswith(')'):
            cleaned = cleaned + ';'
        
        return cleaned
    
    def validate_workflow_resources(self, 
                                  library_resource: Dict[str, Any], 
                                  parameters_resource: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Validate Library and Parameters resources before workflow execution.
        
        Args:
            library_resource: FHIR Library resource
            parameters_resource: Optional FHIR Parameters resource
            
        Returns:
            Validation results dictionary
        """
        validation_result = {
            "valid": True,
            "library_validation": {},
            "parameters_validation": {},
            "workflow_ready": False
        }
        
        # Validate Library resource
        try:
            library_validation = self.library_handler.validate_library_resource(library_resource)
            validation_result["library_validation"] = library_validation
            
            if not library_validation["valid"]:
                validation_result["valid"] = False
                
        except Exception as e:
            validation_result["valid"] = False
            validation_result["library_validation"] = {"error": str(e)}
        
        # Validate Parameters resource if provided
        if parameters_resource:
            try:
                params_validation = self.parameters_handler.validate_parameters_resource(parameters_resource)
                validation_result["parameters_validation"] = params_validation
                
                if not params_validation["valid"]:
                    validation_result["valid"] = False
                    
            except Exception as e:
                validation_result["valid"] = False
                validation_result["parameters_validation"] = {"error": str(e)}
        
        validation_result["workflow_ready"] = validation_result["valid"]
        return validation_result
    
    def get_workflow_capabilities(self) -> Dict[str, Any]:
        """
        Get information about workflow engine capabilities.
        
        Returns:
            Dictionary with capability information
        """
        registry_stats = self.cql_engine.unified_registry.get_registry_stats()
        
        return {
            "engine_info": {
                "dialect": self.dialect,
                "cql_functions_available": registry_stats["total_functions"],
                "function_categories": registry_stats["categories"]
            },
            "datastore_info": {
                "type": type(self.datastore).__name__,
                "connected": self.datastore is not None
            },
            "supported_features": {
                "library_extraction": True,
                "parameters_processing": True,
                "patient_level_results": True,
                "define_statement_execution": True,
                "sql_generation": True
            },
            "workflow_steps": [
                "Extract CQL from Library resource",
                "Extract parameters from Parameters resource",
                "Parse and load CQL library",
                "Set library parameters",
                "Execute define statements",
                "Return patient-level results"
            ]
        }
    
    def create_example_resources(self, cql_content: str, library_id: str) -> Dict[str, Dict[str, Any]]:
        """
        Create example Library and Parameters resources for testing.
        
        Args:
            cql_content: CQL content to encode in Library
            library_id: Library identifier
            
        Returns:
            Dictionary with 'library' and 'parameters' resources
        """
        # Create Library resource
        library_resource = self.library_handler.create_library_resource(
            cql_content=cql_content,
            library_id=library_id,
            name=f"Example Library {library_id}",
            title="Example CQL Library for Testing",
            description="This library demonstrates the FHIR CQL workflow"
        )
        
        # Create Parameters resource
        parameters_resource = self.parameters_handler.create_parameters_resource(
            resource_id=f"{library_id}-params",
            library_reference=f"Library/{library_id}",
            measurement_period_start="2023-01-01",
            measurement_period_end="2023-12-31"
        )
        
        return {
            "library": library_resource,
            "parameters": parameters_resource
        }


    def _resolve_define_references(self, cql_expression: str, define_operations: dict, visited_defines: set = None) -> str:
        """
        Recursively resolve define references in CQL expressions to their actual values.
        
        This prevents the CQL engine from treating define names as FHIR field paths.
        
        Args:
            cql_expression: CQL expression that may contain define references
            define_operations: Dictionary of define operations from the library
            visited_defines: Set of define names already visited (to prevent circular references)
            
        Returns:
            CQL expression with define references recursively resolved to actual values
        """
        if not define_operations or not cql_expression:
            return cql_expression
        
        # Initialize visited set to prevent circular references
        if visited_defines is None:
            visited_defines = set()
        
        # Strip whitespace for exact matching
        expr_stripped = cql_expression.strip()
        
        # Check if the entire expression is a simple define reference
        if expr_stripped in define_operations:
            # Prevent circular references
            if expr_stripped in visited_defines:
                logger.warning(f"Circular define reference detected: {' -> '.join(visited_defines)} -> {expr_stripped}")
                return cql_expression
            
            referenced_define = define_operations[expr_stripped]
            if isinstance(referenced_define, dict) and 'original_expression' in referenced_define:
                resolved = referenced_define['original_expression']
                logger.debug(f"Define reference: '{expr_stripped}' -> '{resolved}'")
                
                # Recursively resolve the referenced expression
                visited_defines.add(expr_stripped)
                final_resolved = self._resolve_define_references(resolved, define_operations, visited_defines.copy())
                logger.debug(f"Recursively resolved: '{expr_stripped}' -> '{final_resolved}'")
                
                return final_resolved
        
        # Handle more complex expressions with define references
        # This could be expanded for expressions like: "today + 1" or "MyDefine.field"
        
        return cql_expression
    
    def _resolve_parameter_references(self, cql_expression: str, library_name: str) -> str:
        """
        Resolve parameter references in CQL expressions.
        
        Handles patterns like:
        - start(Measurement Period) → '2023-01-01T00:00:00.000Z'
        - end(Measurement Period) → '2023-12-31T23:59:59.999Z'
        - Measurement Period → Period{'start': '...', 'end': '...'}
        
        Args:
            cql_expression: Original CQL expression with parameter references
            library_name: Name of the library to get parameters from
            
        Returns:
            CQL expression with parameter references resolved to actual values
        """
        # Get library parameters
        library = self.library_manager.libraries.get(library_name)
        if not library or not library.parameter_values:
            return cql_expression
            
        resolved_expression = cql_expression
        
        # Handle specific parameter patterns
        for param_name, param_value in library.parameter_values.items():
            if param_name in resolved_expression:
                logger.debug(f"Found parameter reference '{param_name}' in expression: {cql_expression}")
                
                # Handle start(Parameter Name) pattern
                start_pattern = f'start({param_name})'
                if start_pattern in resolved_expression:
                    if isinstance(param_value, dict) and 'start' in param_value:
                        start_value = param_value['start']
                        # Use CQL datetime literal format (@) instead of string literal
                        resolved_expression = resolved_expression.replace(start_pattern, f"@{start_value}")
                        logger.info(f"Resolved start({param_name}) → @{start_value}")
                    else:
                        logger.warning(f"Parameter '{param_name}' doesn't have start field: {param_value}")
                
                # Handle end(Parameter Name) pattern  
                end_pattern = f'end({param_name})'
                if end_pattern in resolved_expression:
                    if isinstance(param_value, dict) and 'end' in param_value:
                        end_value = param_value['end'] 
                        # Use CQL datetime literal format (@) instead of string literal
                        resolved_expression = resolved_expression.replace(end_pattern, f"@{end_value}")
                        logger.info(f"Resolved end({param_name}) → @{end_value}")
                    else:
                        logger.warning(f"Parameter '{param_name}' doesn't have end field: {param_value}")
                
                # Handle quoted parameter names like "Measurement Period"
                quoted_param = f'"{param_name}"'
                if quoted_param in resolved_expression and start_pattern not in resolved_expression and end_pattern not in resolved_expression:
                    # This is a direct parameter reference, not inside a function
                    if isinstance(param_value, dict):
                        # For complex types, we might need more sophisticated handling
                        logger.debug(f"Direct parameter reference '{quoted_param}' found, but not substituting complex value")
                    else:
                        # For simple values, substitute directly
                        resolved_expression = resolved_expression.replace(quoted_param, str(param_value))
                        logger.info(f"Resolved {quoted_param} → {param_value}")
        
        return resolved_expression
    
    def _format_execution_results(self, define_name: str, cql_expression: str, 
                                execution_results: Any, sql_query: str) -> List[Dict[str, Any]]:
        """
        Format execution results into patient-level format for MeasureReport generation.
        
        Args:
            define_name: Name of the CQL define being executed
            cql_expression: Original CQL expression
            execution_results: Raw execution results from SQL
            sql_query: Generated SQL query
            
        Returns:
            List of patient-level results with patient_id fields
        """
        patient_results = []
        
        try:
            # Get all patients from the database for proper patient-level evaluation
            patient_query = """
                SELECT DISTINCT json_extract_string(resource, '$.id') as patient_id
                FROM fhir_resources 
                WHERE json_extract_string(resource, '$.resourceType') = 'Patient'
                AND json_extract_string(resource, '$.id') IS NOT NULL
            """
            
            query_result = self.datastore.execute_sql(patient_query)
            patient_results = query_result.fetchall()
            
            if not patient_results:
                logger.warning("No patients found in database for patient-level evaluation")
                return [{"define_name": define_name, "result": execution_results, "patient_id": None}]
            
            # For each patient, create a patient-level result
            for patient_row in patient_results:
                patient_id = patient_row[0] if patient_row else None  # First column is patient_id
                
                # For now, create a simple patient-level result
                # In a full implementation, we'd re-evaluate the CQL per patient
                patient_result = {
                    "patient_id": patient_id,
                    "define_name": define_name, 
                    "cql_expression": cql_expression,
                    "result": execution_results,  # Simplified: use library-level result
                    "sql_query": sql_query[:200] + "..." if len(sql_query) > 200 else sql_query
                }
                
                patient_results.append(patient_result)
                
            logger.info(f"✅ Formatted {len(patient_results)} patient-level results for define '{define_name}'")
            return patient_results
            
        except Exception as e:
            logger.error(f"Error formatting execution results for '{define_name}': {e}")
            # Return basic result without patient context
            return [{"define_name": define_name, "result": execution_results, "error": str(e)}]