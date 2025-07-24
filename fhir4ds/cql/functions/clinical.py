"""
CQL Clinical Functions - Clinical domain function implementations.

This module contains CQL-specific functions that extend beyond FHIRPath,
focusing on clinical domain operations, terminology services, and healthcare-specific logic.
"""

import logging
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, date

from ...fhirpath.parser.ast_nodes import *

logger = logging.getLogger(__name__)

class ClinicalFunctions:
    """
    Clinical domain functions for CQL with terminology integration.
    
    Provides healthcare-specific functionality that extends FHIRPath capabilities,
    now with integrated terminology service support and caching.
    """
    
    def __init__(self, terminology_client=None, db_connection=None, dialect: str = "duckdb"):
        """
        Initialize clinical functions with optional terminology client.
        
        Args:
            terminology_client: Terminology client (uses default if None)
            db_connection: Database connection for caching
            dialect: Database dialect ("duckdb" or "postgresql")
        """
        self.terminology = TerminologyFunctions(
            terminology_client=terminology_client,
            db_connection=db_connection,
            dialect=dialect
        )
    
    def in_valueset(self, code_expr: str, system_expr: str, valueset: str) -> str:
        """
        Check if code is in valueset (CQL: code in "ValueSet") with caching.
        
        Args:
            code_expr: SQL expression for the code
            system_expr: SQL expression for the code system  
            valueset: ValueSet identifier
            
        Returns:
            SQL expression for valueset membership check
        """
        logger.debug(f"Generating valueset check: code in {valueset}")
        
        try:
            # Use terminology functions to generate SQL with caching
            return self.terminology.in_valueset_sql(code_expr, system_expr, valueset)
        except Exception as e:
            logger.error(f"Failed to generate valueset SQL for {valueset}: {e}")
            # Fallback to basic implementation
            return f"-- ValueSet check: {code_expr} in '{valueset}' (error: {e})"
    
    @staticmethod
    def code_in_codesystem(code: str, codesystem: str) -> str:
        """
        Check if code exists in code system.
        
        Args:
            code: Code to check
            codesystem: CodeSystem identifier
            
        Returns:
            SQL expression for code system membership check
        """
        logger.debug(f"Generating codesystem check: {code} in {codesystem}")
        
        # Phase 2: Basic implementation
        return f"-- CodeSystem check: {code} in '{codesystem}'"
    
    @staticmethod
    def overlaps(left_interval: Any, right_interval: Any) -> str:
        """
        Check if two intervals overlap (CQL temporal operations).
        
        Args:
            left_interval: First interval
            right_interval: Second interval
            
        Returns:
            SQL expression for interval overlap check
        """
        logger.debug("Generating interval overlap check")
        
        # Delegate to comprehensive interval handler
        from .interval_functions import CQLIntervalFunctionHandler
        handler = CQLIntervalFunctionHandler()
        return handler.overlaps_proper(left_interval, right_interval)
    
    @staticmethod
    def during(point: Any, interval: Any) -> str:
        """
        Check if point occurs during interval.
        
        Args:
            point: Time point
            interval: Time interval
            
        Returns:
            SQL expression for temporal 'during' check
        """
        logger.debug("Generating 'during' temporal check")
        
        # Delegate to comprehensive interval handler
        from .interval_functions import CQLIntervalFunctionHandler
        handler = CQLIntervalFunctionHandler()
        return handler.during_proper(point, interval)
    
    @staticmethod
    def age_in_years(birth_date: Any) -> str:
        """
        Calculate age in years from birth date.
        
        Args:
            birth_date: Birth date expression
            
        Returns:
            SQL expression for age calculation
        """
        logger.debug("Generating age calculation")
        
        # Generate SQL for age calculation
        # This works across both DuckDB and PostgreSQL
        return f"CAST((CURRENT_DATE - DATE({birth_date})) / 365.25 AS INTEGER)"
    
    @staticmethod
    def age_in_months(birth_date: Any) -> str:
        """
        Calculate age in months from birth date.
        
        Args:
            birth_date: Birth date expression
            
        Returns:
            SQL expression for age in months calculation
        """
        logger.debug("Generating age in months calculation")
        
        return f"CAST(DATEDIFF('month', DATE({birth_date}), CURRENT_DATE) AS INTEGER)"
    
    @staticmethod
    def calculate_period_duration(start_date: Any, end_date: Any, precision: str = "days") -> str:
        """
        Calculate duration between two dates.
        
        Args:
            start_date: Start date expression
            end_date: End date expression  
            precision: Precision (days, months, years)
            
        Returns:
            SQL expression for period duration
        """
        logger.debug(f"Generating period duration calculation with precision: {precision}")
        
        if precision.lower() == "years":
            return f"CAST(DATEDIFF('year', DATE({start_date}), DATE({end_date})) AS INTEGER)"
        elif precision.lower() == "months":
            return f"CAST(DATEDIFF('month', DATE({start_date}), DATE({end_date})) AS INTEGER)"
        else:  # days (default)
            return f"CAST(DATEDIFF('day', DATE({start_date}), DATE({end_date})) AS INTEGER)"

class TerminologyFunctions:
    """
    Terminology service functions for CQL with caching support.
    
    Handles code systems, value sets, and concept operations using
    the terminology service infrastructure with multi-tier caching.
    """
    
    def __init__(self, terminology_client=None, db_connection=None, dialect: str = "duckdb"):
        """
        Initialize terminology functions with optional client.
        
        Args:
            terminology_client: Terminology client (uses default if None)
            db_connection: Database connection for caching
            dialect: Database dialect ("duckdb" or "postgresql")
        """
        self.client = terminology_client
        if not self.client:
            try:
                from ...terminology import get_default_terminology_client
                self.client = get_default_terminology_client(
                    db_connection=db_connection,
                    dialect=dialect
                )
            except Exception as e:
                logger.warning(f"Failed to initialize terminology client: {e}")
                self.client = None
    
    def expand_valueset(self, valueset_id: str, version: str = None) -> List[Dict[str, Any]]:
        """
        Expand a ValueSet to its constituent codes with caching.
        
        Args:
            valueset_id: ValueSet identifier (URL or OID)
            version: Specific version (optional)
            
        Returns:
            List of code dictionaries from the expansion
        """
        logger.debug(f"Expanding ValueSet: {valueset_id}")
        
        if not self.client:
            logger.warning("No terminology client available for valueset expansion")
            return []
        
        try:
            # Call VSAC with caching
            expansion = self.client.expand_valueset(valueset_id, version)
            
            # Extract codes from FHIR expansion format
            codes = []
            expansion_data = expansion.get('expansion', {})
            contains = expansion_data.get('contains', [])
            
            for concept in contains:
                codes.append({
                    'code': concept.get('code'),
                    'system': concept.get('system'),
                    'display': concept.get('display'),
                    'version': concept.get('version')
                })
            
            logger.debug(f"Expanded ValueSet {valueset_id} to {len(codes)} codes")
            return codes
            
        except Exception as e:
            logger.error(f"Failed to expand ValueSet {valueset_id}: {e}")
            return []
    
    def validate_code(self, code: str, system: str, valueset_url: str = None) -> bool:
        """
        Validate if code exists in system/valueset with caching.
        
        Args:
            code: Code to validate
            system: Code system URL
            valueset_url: ValueSet URL (optional)
            
        Returns:
            True if code is valid, False otherwise
        """
        logger.debug(f"Validating code {code} in system {system}")
        
        if not self.client:
            logger.warning("No terminology client available for code validation")
            return False
        
        try:
            # Call VSAC validation with caching
            result = self.client.validate_code(code, system, valueset_url)
            
            # Extract validation result from FHIR Parameters format
            parameters = result.get('parameter', [])
            for param in parameters:
                if param.get('name') == 'result':
                    is_valid = param.get('valueBoolean', False)
                    logger.debug(f"Code validation: {code} in {system} = {is_valid}")
                    return is_valid
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to validate code {code}: {e}")
            return False
    
    def lookup_code(self, code: str, codesystem: str) -> Dict[str, Any]:
        """
        Lookup code details in code system with caching.
        
        Args:
            code: Code to lookup
            codesystem: CodeSystem identifier
            
        Returns:
            Code details dictionary
        """
        logger.debug(f"Looking up code {code} in {codesystem}")
        
        if not self.client:
            logger.warning("No terminology client available for code lookup")
            return {
                'code': code,
                'system': codesystem,
                'display': f"Display for {code}",
                'definition': f"Definition for {code}"
            }
        
        try:
            # Call VSAC lookup with caching
            result = self.client.lookup_code(code, codesystem)
            
            # Extract code details from FHIR Parameters format
            code_details = {
                'code': code,
                'system': codesystem,
                'display': '',
                'definition': ''
            }
            
            parameters = result.get('parameter', [])
            for param in parameters:
                name = param.get('name')
                if name == 'display':
                    code_details['display'] = param.get('valueString', '')
                elif name == 'definition':
                    code_details['definition'] = param.get('valueString', '')
                elif name == 'property':
                    # Handle additional properties if needed
                    pass
            
            return code_details
            
        except Exception as e:
            logger.error(f"Failed to lookup code {code}: {e}")
            return {
                'code': code,
                'system': codesystem,
                'display': f"Display for {code}",
                'definition': f"Definition for {code}"
            }
    
    def subsumes(self, parent_code: str, child_code: str, codesystem: str) -> bool:
        """
        Check if parent code subsumes child code with caching.
        
        Args:
            parent_code: Parent code
            child_code: Child code
            codesystem: CodeSystem identifier
            
        Returns:
            True if parent subsumes child
        """
        logger.debug(f"Checking subsumption: {parent_code} subsumes {child_code} in {codesystem}")
        
        if not self.client:
            logger.warning("No terminology client available for subsumption testing")
            return False
        
        try:
            # Call VSAC subsumption with caching
            result = self.client.subsumes(parent_code, child_code, codesystem)
            
            # Extract subsumption result from FHIR Parameters format
            parameters = result.get('parameter', [])
            for param in parameters:
                if param.get('name') == 'outcome':
                    outcome = param.get('valueCode', '')
                    # FHIR subsumption outcomes: 'equivalent', 'subsumes', 'subsumed-by', 'not-subsumed'
                    is_subsumes = outcome in ['equivalent', 'subsumes']
                    logger.debug(f"Subsumption check: {parent_code} subsumes {child_code} = {is_subsumes}")
                    return is_subsumes
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to check subsumption {parent_code} -> {child_code}: {e}")
            return False
    
    def in_valueset_sql(self, code_expr: str, system_expr: str, valueset_url: str) -> str:
        """
        Generate SQL for CQL 'in' valueset operation with caching support.
        
        This method generates SQL that can be used in CQL expressions to check
        if a code is in a valueset, utilizing the cached expansion.
        
        Args:
            code_expr: SQL expression for the code
            system_expr: SQL expression for the code system
            valueset_url: ValueSet URL
            
        Returns:
            SQL expression for valueset membership check
        """
        logger.debug(f"Generating SQL for valueset check: code in {valueset_url}")
        
        try:
            # Expand the valueset to get all codes
            codes = self.expand_valueset(valueset_url)
            
            if not codes:
                logger.warning(f"No codes found in valueset {valueset_url}")
                return "FALSE"
            
            # Generate SQL IN clause with all codes from the expansion
            code_system_pairs = []
            for concept in codes:
                if concept.get('code') and concept.get('system'):
                    code_system_pairs.append(
                        f"({code_expr} = '{concept['code']}' AND {system_expr} = '{concept['system']}')"
                    )
            
            if not code_system_pairs:
                return "FALSE"
            
            # Create OR condition for all code/system pairs
            sql_condition = " OR ".join(code_system_pairs)
            return f"({sql_condition})"
            
        except Exception as e:
            logger.error(f"Failed to generate SQL for valueset {valueset_url}: {e}")
            return "FALSE"
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get terminology cache statistics.
        
        Returns:
            Cache statistics dictionary
        """
        if self.client and hasattr(self.client, 'get_cache_stats'):
            return self.client.get_cache_stats()
        return {"caching_enabled": False}

class ClinicalLogicFunctions:
    """
    Clinical logic and decision support functions.
    
    Provides healthcare-specific logic operations and clinical decision support.
    """
    
    @staticmethod
    def most_recent(observations: List[Any], date_field: str = "effectiveDateTime") -> str:
        """
        Get most recent observation from a list.
        
        Args:
            observations: List of observations
            date_field: Field containing the date
            
        Returns:
            SQL expression for most recent observation
        """
        logger.debug("Generating most recent observation query")
        
        # Generate SQL that gets the observation with the latest date
        return f"""
        (SELECT obs.* FROM ({observations}) obs 
         ORDER BY DATE(json_extract_string(obs.value, '$.{date_field}')) DESC 
         LIMIT 1)
        """.strip()
    
    @staticmethod
    def latest_value(measurements: List[Any], value_field: str = "valueQuantity.value") -> str:
        """
        Get the latest measured value from a series.
        
        Args:
            measurements: List of measurements
            value_field: Field containing the value
            
        Returns:
            SQL expression for latest value
        """
        logger.debug("Generating latest value query")
        
        return f"""
        (SELECT json_extract_string(meas.value, '$.{value_field}') 
         FROM ({measurements}) meas 
         ORDER BY DATE(json_extract_string(meas.value, '$.effectiveDateTime')) DESC 
         LIMIT 1)
        """.strip()
    
    @staticmethod
    def average_value(measurements: List[Any], value_field: str = "valueQuantity.value") -> str:
        """
        Calculate average value from measurements.
        
        Args:
            measurements: List of measurements
            value_field: Field containing the value
            
        Returns:
            SQL expression for average value
        """
        logger.debug("Generating average value calculation")
        
        return f"""
        (SELECT AVG(CAST(json_extract_string(meas.value, '$.{value_field}') AS DECIMAL))
         FROM ({measurements}) meas)
        """.strip()
    
    @staticmethod
    def within_normal_range(value: Any, normal_low: float, normal_high: float) -> str:
        """
        Check if value is within normal range.
        
        Args:
            value: Value to check
            normal_low: Lower bound of normal range
            normal_high: Upper bound of normal range
            
        Returns:
            SQL expression for normal range check
        """
        logger.debug(f"Generating normal range check: {normal_low} <= value <= {normal_high}")
        
        return f"(CAST({value} AS DECIMAL) >= {normal_low} AND CAST({value} AS DECIMAL) <= {normal_high})"