"""
CQL Clinical Functions - Clinical domain function implementations.

This module contains CQL-specific functions that extend beyond FHIRPath,
focusing on clinical domain operations, terminology services, and healthcare-specific logic.
"""

import logging
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, date

from ...fhirpath.parser.ast_nodes import *

# Import code validation and appropriateness framework
try:
    from ..navigation.code_validator import CodeValidator, ValidationResult, create_validator
    from ..navigation.code_appropriateness import (
        ClinicalContext, SpecificityLevel, AppropriatenessScore,
        CodeAppropriatenessScorer, create_appropriateness_scorer
    )
    CODE_VALIDATION_AVAILABLE = True
except ImportError:
    CODE_VALIDATION_AVAILABLE = False
    # Define dummy classes for graceful degradation
    class CodeValidator:
        pass
    class ValidationResult:
        VALID = "valid"
        INVALID = "invalid"
        UNKNOWN = "unknown"
    class ClinicalContext:
        GENERAL = "general"
        DIAGNOSIS = "diagnosis"
        PROCEDURE = "procedure"
    class AppropriatenessScore:
        pass
    class CodeAppropriatenessScorer:
        pass


# Custom Exception Classes for Clinical Functions
class ClinicalFunctionError(Exception):
    """Base exception for clinical function errors."""
    pass


class TerminologyError(ClinicalFunctionError):
    """Exception for terminology-related errors (ValueSet, CodeSystem issues)."""
    pass


class ValidationError(ClinicalFunctionError):
    """Exception for clinical data validation errors."""
    pass


class DataFormatError(ClinicalFunctionError):
    """Exception for clinical data format and parsing errors."""
    pass


class NetworkError(ClinicalFunctionError):
    """Exception for network-related terminology service failures."""
    pass


class SecurityError(ClinicalFunctionError):
    """Exception for security-related issues in clinical functions."""
    pass


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
    
    def in_valueset(self, code_expr: str, system_expr: str, valueset: str, 
                   version: str = None) -> str:
        """
        Check if code is in valueset (CQL: code in "ValueSet") with enhanced caching.
        
        Args:
            code_expr: SQL expression for the code
            system_expr: SQL expression for the code system  
            valueset: ValueSet identifier
            version: Specific version of ValueSet (optional)
            
        Returns:
            SQL expression for valueset membership check
        """
        logger.debug(f"Generating enhanced valueset check: code in {valueset}")
        
        try:
            # Use enhanced terminology functions to generate SQL with caching
            return self.terminology.in_valueset_sql(code_expr, system_expr, valueset, version)
        except (ImportError, ModuleNotFoundError) as e:
            logger.error(f"Missing dependency for terminology services: {e}")
            raise NetworkError(f"Terminology service dependencies not available: {e}")
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Network error accessing valueset {valueset}: {e}")
            raise NetworkError(f"Failed to connect to terminology service for {valueset}: {e}")
        except (ValueError, KeyError, TypeError) as e:
            logger.error(f"Invalid data format for valueset {valueset}: {e}")
            raise DataFormatError(f"Invalid valueset format for {valueset}: {e}")
        except TerminologyError:
            # Re-raise terminology errors as-is
            raise
        except SecurityError:
            # Re-raise security errors as-is 
            raise
        except Exception as e:
            logger.error(f"Unexpected error generating valueset SQL for {valueset}: {e}")
            # Return safe fallback only for truly unexpected errors
            return f"({code_expr} IS NOT NULL AND {system_expr} IS NOT NULL AND FALSE /* ValueSet error: {valueset} */)"
    
    def batch_in_valueset(self, code_system_pairs: List[str], valueset: str) -> str:
        """
        Batch check multiple codes against a valueset for performance.
        
        Args:
            code_system_pairs: List of "code|system" strings
            valueset: ValueSet identifier
            
        Returns:
            SQL expression for batch valueset membership check
        """
        logger.debug(f"Generating batch valueset check for {len(code_system_pairs)} codes")
        
        try:
            # Parse code|system pairs
            parsed_pairs = []
            for pair in code_system_pairs:
                if '|' in pair:
                    code, system = pair.split('|', 1)
                    parsed_pairs.append({'code': code, 'system': system})
            
            if not parsed_pairs:
                return "FALSE"
            
            # Get validation results
            validation_results = self.terminology.batch_validate_codes(parsed_pairs, valueset)
            
            # Generate SQL condition based on results
            valid_pairs = [pair for pair, is_valid in validation_results.items() if is_valid]
            
            if not valid_pairs:
                return "FALSE"
            
            # Convert back to SQL conditions
            conditions = []
            for pair in valid_pairs:
                code, system = pair.split('|', 1)
                conditions.append(f"(code = '{code}' AND system = '{system}')")
            
            return f"({' OR '.join(conditions)})"
            
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid data format in batch valueset operation: {e}")
            raise DataFormatError(f"Invalid code/system format in batch operation: {e}")
        except TerminologyError:
            # Re-raise terminology errors as-is
            raise
        except NetworkError:
            # Re-raise network errors as-is
            raise
        except Exception as e:
            logger.error(f"Unexpected error in batch valueset SQL generation: {e}")
            return "FALSE"
    
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
    
    def get_terminology_cache_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive terminology cache statistics.
        
        Returns:
            Detailed cache statistics
        """
        return self.terminology.get_cache_stats()
    
    def subsumes_code(self, parent_code: str, parent_system: str, 
                     child_code: str, child_system: str) -> str:
        """
        Check if parent code subsumes child code (SQL generation).
        
        Args:
            parent_code: Parent code
            parent_system: Parent code system
            child_code: Child code
            child_system: Child code system
            
        Returns:
            SQL expression for subsumption check
        """
        logger.debug(f"Generating subsumption SQL: {parent_code} subsumes {child_code}")
        
        try:
            # Use terminology functions for subsumption
            if parent_system == child_system:
                result = self.terminology.subsumes(parent_code, child_code, parent_system)
                return "TRUE" if result else "FALSE"
            else:
                # Different code systems - generally false unless there's a mapping
                return "FALSE"
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid parameters for subsumption check: {e}")
            raise ValidationError(f"Invalid code parameters for subsumption: {e}")
        except TerminologyError:
            # Re-raise terminology errors as-is
            raise
        except NetworkError:
            # Re-raise network errors as-is
            raise
        except Exception as e:
            logger.error(f"Unexpected error in subsumption check: {e}")
            return "FALSE"
    
    def expand_valueset_to_sql_condition(self, valueset_url: str, 
                                       code_column: str = "code", 
                                       system_column: str = "system") -> str:
        """
        Generate SQL WHERE condition from ValueSet expansion.
        
        Args:
            valueset_url: ValueSet URL to expand
            code_column: Column name for codes
            system_column: Column name for systems
            
        Returns:
            SQL WHERE condition for ValueSet membership
        """
        logger.debug(f"Generating SQL condition for ValueSet {valueset_url}")
        
        try:
            return self.terminology.in_valueset_sql(code_column, system_column, valueset_url)
        except Exception as e:
            logger.error(f"Failed to generate SQL condition for {valueset_url}: {e}")
            return "FALSE"

class TerminologyFunctions:
    """
    Terminology service functions for CQL with caching support.
    
    Handles code systems, value sets, and concept operations using
    the terminology service infrastructure with multi-tier caching.
    """
    
    def __init__(self, terminology_client=None, db_connection=None, dialect: str = "duckdb", 
                 enable_code_validation: bool = False, validator_type: str = "vsac",
                 enable_appropriateness_scoring: bool = False, scoring_weights: Optional[Dict[str, float]] = None):
        """
        Initialize terminology functions with optional client, code validation, and appropriateness scoring.
        
        Args:
            terminology_client: Terminology client (uses default if None)
            db_connection: Database connection for caching
            dialect: Database dialect ("duckdb" or "postgresql")
            enable_code_validation: Enable real-time code validation against authoritative sources
            validator_type: Type of validator to use ("vsac" or "fhir")
            enable_appropriateness_scoring: Enable code appropriateness scoring
            scoring_weights: Custom weights for appropriateness scoring components
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
        
        # Initialize code validator if enabled
        self.code_validator = None
        self.enable_code_validation = enable_code_validation
        if enable_code_validation and CODE_VALIDATION_AVAILABLE:
            try:
                self.code_validator = create_validator(validator_type)
                logger.info(f"Initialized code validator: {validator_type}")
            except Exception as e:
                logger.warning(f"Failed to initialize code validator: {e}")
                self.enable_code_validation = False
        elif enable_code_validation and not CODE_VALIDATION_AVAILABLE:
            logger.warning("Code validation requested but navigation module not available")
            self.enable_code_validation = False
        
        # Initialize appropriateness scorer if enabled
        self.appropriateness_scorer = None
        self.enable_appropriateness_scoring = enable_appropriateness_scoring
        if enable_appropriateness_scoring and CODE_VALIDATION_AVAILABLE:
            try:
                self.appropriateness_scorer = create_appropriateness_scorer(
                    "default", 
                    code_validator=self.code_validator,
                    weights=scoring_weights
                )
                logger.info("Initialized code appropriateness scorer")
            except Exception as e:
                logger.warning(f"Failed to initialize appropriateness scorer: {e}")
                self.enable_appropriateness_scoring = False
        elif enable_appropriateness_scoring and not CODE_VALIDATION_AVAILABLE:
            logger.warning("Code appropriateness scoring requested but navigation module not available")
            self.enable_appropriateness_scoring = False
    
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
    
    def validate_code_with_details(self, code: str, system: str, value_set_url: str = None) -> Dict[str, Any]:
        """
        Validate individual code against authoritative terminology source with detailed results.
        
        This method provides comprehensive code validation using the individual code validation
        framework, returning detailed validation results including display names, issues, and
        performance metrics.
        
        Args:
            code: Code to validate (e.g., "M25.50")
            system: Code system URI (e.g., "http://hl7.org/fhir/sid/icd-10-cm")
            value_set_url: Optional ValueSet URL for context-specific validation
            
        Returns:
            Dict containing validation details:
            - valid: Boolean indicating if code is valid
            - result: ValidationResult enum value
            - display: Official display name for the code (if available)
            - message: Human-readable validation message
            - issues: List of validation issues/warnings
            - response_time_ms: API response time in milliseconds
            - cached: Whether result was retrieved from cache
        """
        if not self.enable_code_validation or not self.code_validator:
            # Fallback to basic validation without detailed results
            is_valid = self.validate_code(code, system, value_set_url)
            return {
                'valid': is_valid,
                'result': ValidationResult.VALID if is_valid else ValidationResult.INVALID,
                'display': None,
                'message': 'Basic validation (detailed validation not available)',
                'issues': [],
                'response_time_ms': None,
                'cached': False
            }
        
        try:
            validation_result = self.code_validator.validate_code(code, system, value_set_url)
            
            return {
                'valid': validation_result.result == ValidationResult.VALID,
                'result': validation_result.result.value,
                'display': validation_result.display,
                'message': validation_result.message,
                'issues': validation_result.issues,
                'response_time_ms': validation_result.response_time_ms,
                'cached': validation_result.cached
            }
            
        except Exception as e:
            logger.error(f"Code validation failed for {system}|{code}: {e}")
            return {
                'valid': False,
                'result': ValidationResult.UNKNOWN.value,
                'display': None,
                'message': f'Validation error: {str(e)}',
                'issues': [str(e)],
                'response_time_ms': None,
                'cached': False
            }
    
    def validate_codes_batch_with_details(self, codes: List[tuple]) -> List[Dict[str, Any]]:
        """
        Batch validate multiple codes with detailed results for efficiency.
        
        Args:
            codes: List of (code, system, value_set_url) tuples
            
        Returns:
            List of validation result dictionaries (same format as validate_code_with_details)
        """
        if not self.enable_code_validation or not self.code_validator:
            # Fallback to individual basic validation
            results = []
            for code, system, value_set_url in codes:
                is_valid = self.validate_code(code, system, value_set_url)
                results.append({
                    'valid': is_valid,
                    'result': ValidationResult.VALID if is_valid else ValidationResult.INVALID,
                    'display': None,
                    'message': 'Basic validation (detailed validation not available)',
                    'issues': [],
                    'response_time_ms': None,
                    'cached': False
                })
            return results
        
        try:
            validation_results = self.code_validator.validate_codes_batch(codes)
            
            detailed_results = []
            for validation_result in validation_results:
                detailed_results.append({
                    'valid': validation_result.result == ValidationResult.VALID,
                    'result': validation_result.result.value,
                    'display': validation_result.display,
                    'message': validation_result.message,
                    'issues': validation_result.issues,
                    'response_time_ms': validation_result.response_time_ms,
                    'cached': validation_result.cached
                })
            
            return detailed_results
            
        except Exception as e:
            logger.error(f"Batch code validation failed: {e}")
            # Return error results for all codes
            return [
                {
                    'valid': False,
                    'result': ValidationResult.UNKNOWN.value,
                    'display': None,
                    'message': f'Batch validation error: {str(e)}',
                    'issues': [str(e)],
                    'response_time_ms': None,
                    'cached': False
                }
                for _ in codes
            ]
    
    def score_code_appropriateness(self, code: str, system: str, clinical_context: str = "general",
                                   additional_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Score clinical code appropriateness for a given context.
        
        This method evaluates the appropriateness of a clinical code based on multiple
        dimensions including specificity, currency, context relevance, and validity.
        
        Args:
            code: Code to score (e.g., "M25.50")
            system: Code system URI (e.g., "http://hl7.org/fhir/sid/icd-10-cm")
            clinical_context: Clinical context ("diagnosis", "procedure", "laboratory", etc.)
            additional_context: Additional context information for scoring
            
        Returns:
            Dict containing appropriateness scoring details:
            - overall_score: Overall appropriateness score (0.0 to 1.0)
            - specificity_score: Code specificity/granularity score
            - currency_score: Code currency/modernity score
            - context_score: Context relevance score
            - validity_score: Code validity score
            - specificity_level: Categorized specificity level
            - issues: List of identified appropriateness issues
            - recommendations: List of improvement recommendations
        """
        if not self.enable_appropriateness_scoring or not self.appropriateness_scorer:
            return {
                'overall_score': 0.5,
                'specificity_score': 0.5,
                'currency_score': 0.5,
                'context_score': 0.5,
                'validity_score': 0.5,
                'specificity_level': 'unknown',
                'issues': ['Appropriateness scoring not available'],
                'recommendations': ['Enable appropriateness scoring for detailed analysis'],
                'scoring_available': False
            }
        
        try:
            # Map string context to enum
            context_mapping = {
                'general': ClinicalContext.GENERAL,
                'diagnosis': ClinicalContext.DIAGNOSIS,
                'procedure': ClinicalContext.PROCEDURE,
                'medication': ClinicalContext.MEDICATION,
                'laboratory': ClinicalContext.LABORATORY,
                'observation': ClinicalContext.OBSERVATION,
                'encounter': ClinicalContext.ENCOUNTER
            }
            
            clinical_context_enum = context_mapping.get(clinical_context.lower(), ClinicalContext.GENERAL)
            
            # Score the code
            score = self.appropriateness_scorer.score_code(
                code, system, clinical_context_enum, additional_context
            )
            
            return {
                'overall_score': score.overall_score,
                'specificity_score': score.specificity_score,
                'currency_score': score.currency_score,
                'context_score': score.context_score,
                'validity_score': score.validity_score,
                'specificity_level': score.specificity_level.value,
                'issues': score.issues,
                'recommendations': score.recommendations,
                'scoring_available': True,
                'computed_at': score.computed_at.isoformat(),
                'scoring_method': score.scoring_method
            }
            
        except Exception as e:
            logger.error(f"Code appropriateness scoring failed for {system}|{code}: {e}")
            return {
                'overall_score': 0.0,
                'specificity_score': 0.0,
                'currency_score': 0.0,
                'context_score': 0.0,
                'validity_score': 0.0,
                'specificity_level': 'unknown',
                'issues': [f'Scoring error: {str(e)}'],
                'recommendations': ['Check code format and system URL'],
                'scoring_available': True,
                'error': str(e)
            }
    
    def score_codes_appropriateness_batch(self, codes: List[tuple]) -> List[Dict[str, Any]]:
        """
        Batch score multiple codes for appropriateness efficiency.
        
        Args:
            codes: List of (code, system, clinical_context) tuples
            
        Returns:
            List of appropriateness scoring result dictionaries
        """
        if not self.enable_appropriateness_scoring or not self.appropriateness_scorer:
            # Return default scores for all codes
            return [
                {
                    'overall_score': 0.5,
                    'specificity_score': 0.5,
                    'currency_score': 0.5,
                    'context_score': 0.5,
                    'validity_score': 0.5,
                    'specificity_level': 'unknown',
                    'issues': ['Appropriateness scoring not available'],
                    'recommendations': ['Enable appropriateness scoring for detailed analysis'],
                    'scoring_available': False
                }
                for _ in codes
            ]
        
        try:
            # Convert string contexts to enums
            context_mapping = {
                'general': ClinicalContext.GENERAL,
                'diagnosis': ClinicalContext.DIAGNOSIS,
                'procedure': ClinicalContext.PROCEDURE,
                'medication': ClinicalContext.MEDICATION,
                'laboratory': ClinicalContext.LABORATORY,
                'observation': ClinicalContext.OBSERVATION,
                'encounter': ClinicalContext.ENCOUNTER
            }
            
            # Prepare codes with enum contexts
            enum_codes = []
            for code, system, clinical_context in codes:
                context_enum = context_mapping.get(clinical_context.lower(), ClinicalContext.GENERAL)
                enum_codes.append((code, system, context_enum))
            
            # Batch score the codes
            scores = self.appropriateness_scorer.score_codes_batch(enum_codes)
            
            # Convert to dictionaries
            results = []
            for score in scores:
                results.append({
                    'overall_score': score.overall_score,
                    'specificity_score': score.specificity_score,
                    'currency_score': score.currency_score,
                    'context_score': score.context_score,
                    'validity_score': score.validity_score,
                    'specificity_level': score.specificity_level.value,
                    'issues': score.issues,
                    'recommendations': score.recommendations,
                    'scoring_available': True,
                    'computed_at': score.computed_at.isoformat(),
                    'scoring_method': score.scoring_method
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Batch code appropriateness scoring failed: {e}")
            # Return error results for all codes
            return [
                {
                    'overall_score': 0.0,
                    'specificity_score': 0.0,
                    'currency_score': 0.0,
                    'context_score': 0.0,
                    'validity_score': 0.0,
                    'specificity_level': 'unknown',
                    'issues': [f'Batch scoring error: {str(e)}'],
                    'recommendations': ['Check code formats and system URLs'],
                    'scoring_available': True,
                    'error': str(e)
                }
                for _ in codes
            ]
    
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
    
    def in_valueset_sql(self, code_expr: str, system_expr: str, valueset_url: str, 
                        version: str = None, use_cache: bool = True) -> str:
        """
        Generate SQL for CQL 'in' valueset operation with enhanced caching and error handling.
        
        This method generates SQL that can be used in CQL expressions to check
        if a code is in a valueset, utilizing the cached expansion and optimized SQL generation.
        
        Args:
            code_expr: SQL expression for the code
            system_expr: SQL expression for the code system
            valueset_url: ValueSet URL
            version: Specific version of ValueSet (optional)
            use_cache: Whether to use cached expansion (default: True)
            
        Returns:
            SQL expression for valueset membership check
        """
        logger.debug(f"Generating enhanced SQL for valueset check: code in {valueset_url}")
        
        try:
            # Expand the valueset to get all codes
            codes = self.expand_valueset(valueset_url, version)
            
            if not codes:
                logger.warning(f"No codes found in valueset {valueset_url}")
                # Return a condition that will be false but doesn't break SQL
                return f"({code_expr} IS NULL AND {system_expr} IS NULL AND FALSE)"
            
            # Optimize for small vs large valuesets
            if len(codes) <= 50:
                # Small valueset - use direct OR conditions
                return self._generate_small_valueset_sql(codes, code_expr, system_expr)
            else:
                # Large valueset - use optimized IN clauses or temp table approach
                return self._generate_large_valueset_sql(codes, code_expr, system_expr, valueset_url)
            
        except Exception as e:
            logger.error(f"Failed to generate SQL for valueset {valueset_url}: {e}")
            # Return safe fallback that doesn't break query execution
            return f"({code_expr} IS NOT NULL AND {system_expr} IS NOT NULL AND FALSE /* ValueSet error: {e} */)"
    
    def _generate_small_valueset_sql(self, codes: List[Dict], code_expr: str, system_expr: str) -> str:
        """Generate SQL for small valuesets using OR conditions with parameterized queries."""
        # Validate input expressions to prevent SQL injection
        if not self._is_safe_sql_expression(code_expr) or not self._is_safe_sql_expression(system_expr):
            logger.error(f"Potentially unsafe SQL expressions detected: code_expr='{code_expr}', system_expr='{system_expr}'")
            return "(FALSE /* Unsafe SQL expressions rejected */)"
        
        code_system_pairs = []
        for concept in codes:
            if concept.get('code') and concept.get('system'):
                # Use proper SQL escaping instead of simple string replacement
                escaped_code = self._escape_sql_string(concept['code'])
                escaped_system = self._escape_sql_string(concept['system'])
                code_system_pairs.append(
                    f"({code_expr} = {escaped_code} AND {system_expr} = {escaped_system})"
                )
        
        if not code_system_pairs:
            return f"({code_expr} IS NULL AND {system_expr} IS NULL AND FALSE)"
        
        # Create OR condition for all code/system pairs
        sql_condition = " OR ".join(code_system_pairs)
        return f"({sql_condition})"
    
    def _generate_large_valueset_sql(self, codes: List[Dict], code_expr: str, system_expr: str, 
                                   valueset_url: str) -> str:
        """Generate optimized SQL for large valuesets using CTE patterns instead of massive OR clauses."""
        # Validate input expressions to prevent SQL injection
        if not self._is_safe_sql_expression(code_expr) or not self._is_safe_sql_expression(system_expr):
            logger.error(f"Potentially unsafe SQL expressions detected: code_expr='{code_expr}', system_expr='{system_expr}'")
            return "(FALSE /* Unsafe SQL expressions rejected */)"
        
        # Group codes by system for better performance
        codes_by_system = {}
        for concept in codes:
            if concept.get('code') and concept.get('system'):
                system = concept['system']
                if system not in codes_by_system:
                    codes_by_system[system] = []
                codes_by_system[system].append(concept['code'])
        
        if not codes_by_system:
            return f"({code_expr} IS NULL AND {system_expr} IS NULL AND FALSE)"
        
        # Determine CTE optimization strategy based on size and dialect
        total_codes = sum(len(code_list) for code_list in codes_by_system.values())
        
        if total_codes > 1000:
            # For very large ValueSets, use temporary table approach with CTEs
            return self._generate_cte_temp_table_sql(codes_by_system, code_expr, system_expr, valueset_url)
        else:
            # For moderately large ValueSets, use CTE with VALUES clause
            return self._generate_cte_values_sql(codes_by_system, code_expr, system_expr)
    
    def _is_safe_sql_expression(self, expression: str) -> bool:
        """
        Validate that SQL expression is safe to use in queries.
        
        Args:
            expression: SQL expression to validate
            
        Returns:
            True if expression appears safe, False otherwise
        """
        if not expression or not isinstance(expression, str):
            return False
        
        # Check for potentially dangerous SQL keywords and characters
        dangerous_patterns = [
            ';',  # Statement terminator
            '--',  # SQL comments
            '/*',  # Block comments start
            '*/',  # Block comments end
            'exec',  # Execute statements
            'sp_',   # Stored procedures
            'xp_',   # Extended stored procedures
            'drop',  # Drop statements
            'delete',  # Delete statements  
            'insert',  # Insert statements
            'update',  # Update statements
            'create',  # Create statements
            'alter',   # Alter statements
            'union',   # Union statements (outside of expected context)
            '@@',      # Global variables
            'char(',   # Character conversion functions
            'ascii(',  # ASCII conversion functions
        ]
        
        expression_lower = expression.lower()
        
        # Check for dangerous patterns
        for pattern in dangerous_patterns:
            if pattern in expression_lower:
                logger.warning(f"Potentially dangerous SQL pattern '{pattern}' found in expression: {expression}")
                return False
        
        # Expression appears safe
        return True
    
    def _escape_sql_string(self, value: str) -> str:
        """
        Properly escape SQL string literals.
        
        Args:
            value: String value to escape
            
        Returns:
            Properly escaped SQL string literal
        """
        if value is None:
            return "NULL"
        
        # Convert to string and escape single quotes
        escaped_value = str(value).replace("'", "''")
        
        # Also escape backslashes for databases that interpret them
        escaped_value = escaped_value.replace("\\", "\\\\")
        
        return f"'{escaped_value}'"
    
    def _generate_cte_values_sql(self, codes_by_system: Dict[str, List[str]], 
                                code_expr: str, system_expr: str) -> str:
        """
        Generate CTE-based SQL for moderately large ValueSets using VALUES clause.
        
        This approach uses CTEs with VALUES clause to avoid massive OR conditions,
        providing better performance and readability for medium-sized ValueSets.
        
        Args:
            codes_by_system: Dictionary mapping system URLs to lists of codes
            code_expr: SQL expression for the code
            system_expr: SQL expression for the code system
            
        Returns:
            CTE-based SQL expression for valueset membership check
        """
        # Create VALUES clauses for each system
        values_clauses = []
        
        for system, code_list in codes_by_system.items():
            escaped_system = self._escape_sql_string(system)
            
            # Create VALUES tuples for this system
            system_values = []
            for code in code_list:
                escaped_code = self._escape_sql_string(code)
                system_values.append(f"({escaped_code}, {escaped_system})")
            
            # Group codes in batches to avoid SQL query length limits
            batch_size = 100
            for i in range(0, len(system_values), batch_size):
                batch = system_values[i:i + batch_size]
                values_clause = ",\n        ".join(batch)
                values_clauses.append(f"""
    SELECT code_val, system_val FROM (
        VALUES
        {values_clause}
    ) AS codes_batch(code_val, system_val)""")
        
        # Combine all VALUES clauses with UNION ALL
        union_query = "\n    UNION ALL".join(values_clauses)
        
        # Generate the final CTE-based query
        cte_sql = f"""
EXISTS (
    WITH valueset_codes AS ({union_query}
    )
    SELECT 1 
    FROM valueset_codes 
    WHERE {code_expr} = code_val 
    AND {system_expr} = system_val
)"""
        
        return cte_sql.strip()
    
    def _generate_cte_temp_table_sql(self, codes_by_system: Dict[str, List[str]], 
                                    code_expr: str, system_expr: str, valueset_url: str) -> str:
        """
        Generate CTE-based SQL for very large ValueSets using optimized temporary table approach.
        
        For extremely large ValueSets (1000+ codes), this uses CTEs with efficient
        batching and system-level optimization to avoid query size limits.
        
        Args:
            codes_by_system: Dictionary mapping system URLs to lists of codes
            code_expr: SQL expression for the code
            system_expr: SQL expression for the code system
            valueset_url: ValueSet URL (used for caching key)
            
        Returns:
            Optimized CTE-based SQL for very large valueset membership check
        """
        logger.info(f"Using CTE optimization for large ValueSet {valueset_url} with {sum(len(codes) for codes in codes_by_system.values())} total codes")
        
        # For very large ValueSets, create system-specific CTEs
        system_ctes = []
        
        for system_idx, (system, code_list) in enumerate(codes_by_system.items()):
            escaped_system = self._escape_sql_string(system)
            
            # Create batched VALUES for this system
            batch_size = 200  # Larger batches for very large sets
            system_batches = []
            
            for batch_idx, i in enumerate(range(0, len(code_list), batch_size)):
                batch = code_list[i:i + batch_size]
                escaped_codes = [self._escape_sql_string(code) for code in batch]
                
                values_tuples = [f"({code})" for code in escaped_codes]
                values_clause = ",\n            ".join(values_tuples)
                
                batch_cte = f"""
        system_{system_idx}_batch_{batch_idx} AS (
            SELECT code_val FROM (
                VALUES
                {values_clause}
            ) AS batch(code_val)
        )"""
                system_batches.append(f"system_{system_idx}_batch_{batch_idx}")
                system_ctes.append(batch_cte)
            
            # Create union CTE for this system
            if len(system_batches) > 1:
                union_parts = [f"SELECT code_val FROM {batch_name}" for batch_name in system_batches]
                union_query = "\n            UNION ALL\n            ".join(union_parts)
                system_union_cte = f"""
        system_{system_idx}_codes AS (
            {union_query}
        )"""
                system_ctes.append(system_union_cte)
            
            # Add system validation CTE
            final_system_cte_name = f"system_{system_idx}_codes" if len(system_batches) > 1 else system_batches[0]
            system_check_cte = f"""
        system_{system_idx}_check AS (
            SELECT 1 as found
            FROM {final_system_cte_name}
            WHERE {code_expr} = code_val AND {system_expr} = {escaped_system}
            LIMIT 1
        )"""
            system_ctes.append(system_check_cte)
        
        # Create final result CTE
        system_checks = [f"system_{i}_check" for i in range(len(codes_by_system))]
        union_checks = "\n            UNION ALL\n            ".join([f"SELECT found FROM {check}" for check in system_checks])
        
        final_cte = f"""
        final_check AS (
            {union_checks}
        )"""
        system_ctes.append(final_cte)
        
        # Combine all CTEs
        all_ctes = ",".join(system_ctes)
        
        cte_sql = f"""
EXISTS (
    WITH {all_ctes}
    SELECT 1 FROM final_check WHERE found = 1 LIMIT 1
)"""
        
        return cte_sql.strip()
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get terminology cache statistics.
        
        Returns:
            Cache statistics dictionary
        """
        if self.client and hasattr(self.client, 'get_cache_stats'):
            return self.client.get_cache_stats()
        return {"caching_enabled": False}
    
    def batch_validate_codes(self, code_system_pairs: List[Dict[str, str]], 
                            valueset_url: str = None) -> Dict[str, bool]:
        """
        Batch validate multiple codes for better performance.
        
        Args:
            code_system_pairs: List of {'code': str, 'system': str} dictionaries
            valueset_url: Optional ValueSet URL for additional validation
            
        Returns:
            Dictionary mapping code|system to validation result
        """
        logger.debug(f"Batch validating {len(code_system_pairs)} codes")
        
        results = {}
        
        for pair in code_system_pairs:
            code = pair.get('code')
            system = pair.get('system')
            
            if not code or not system:
                continue
            
            key = f"{code}|{system}"
            try:
                results[key] = self.validate_code(code, system, valueset_url)
            except Exception as e:
                logger.error(f"Failed to validate {key}: {e}")
                results[key] = False
        
        return results
    
    def expand_valueset_with_filters(self, valueset_url: str, filters: Dict[str, Any] = None,
                                   include_inactive: bool = False) -> List[Dict[str, Any]]:
        """
        Expand valueset with additional filtering options.
        
        Args:
            valueset_url: ValueSet URL
            filters: Additional filters (e.g., {'property': 'status', 'value': 'active'})
            include_inactive: Whether to include inactive codes
            
        Returns:
            Filtered list of codes
        """
        logger.debug(f"Expanding valueset {valueset_url} with filters")
        
        # Get base expansion
        codes = self.expand_valueset(valueset_url)
        
        if not filters and include_inactive:
            return codes
        
        # Apply filters
        filtered_codes = []
        for concept in codes:
            # Apply custom filters if provided
            if filters:
                include_code = True
                for filter_key, filter_value in filters.items():
                    concept_value = concept.get(filter_key)
                    if concept_value != filter_value:
                        include_code = False
                        break
                
                if not include_code:
                    continue
            
            # Handle inactive codes
            if not include_inactive:
                # Assume codes are active unless explicitly marked inactive
                status = concept.get('status', 'active')
                if status == 'inactive':
                    continue
            
            filtered_codes.append(concept)
        
        logger.debug(f"Filtered to {len(filtered_codes)} codes")
        return filtered_codes
    
    def get_concept_relationships(self, code: str, system: str, 
                                relationship_type: str = 'all') -> List[Dict[str, Any]]:
        """
        Get concept relationships (subsumption, equivalence, etc.).
        
        Args:
            code: Source code
            system: Code system URL
            relationship_type: Type of relationships ('parent', 'child', 'equivalent', 'all')
            
        Returns:
            List of related concepts
        """
        logger.debug(f"Getting {relationship_type} relationships for {code} in {system}")
        
        if not self.client:
            logger.warning("No terminology client available for concept relationships")
            return []
        
        try:
            # This would require extension of the base client interface
            if hasattr(self.client, 'get_concept_relationships'):
                return self.client.get_concept_relationships(code, system, relationship_type)
            
            # Fallback: use basic subsumption checking for parent/child relationships
            relationships = []
            
            if relationship_type in ['parent', 'all']:
                # This would need to be implemented based on specific terminology service
                pass
            
            if relationship_type in ['child', 'all']:
                # This would need to be implemented based on specific terminology service
                pass
            
            return relationships
            
        except Exception as e:
            logger.error(f"Failed to get concept relationships for {code}: {e}")
            return []

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