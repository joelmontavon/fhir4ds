"""
CQL to CTE Fragment Converter

This module converts CQL expressions to CTE fragments that can be combined
into monolithic queries, building on existing CQL processing infrastructure
while enabling the replacement of individual queries with comprehensive CTEs.

The converter integrates with existing resource type detection, terminology 
services, and pipeline patterns to ensure compatibility and consistency.
"""

from typing import Dict, List, Optional, Set, Any, Tuple
import re
import logging
from .cte_fragment import CTEFragment

logger = logging.getLogger(__name__)


class EnhancedDependencyDetector:
    """Enhanced dependency detection for CQL define statements - Task 3.2 Enhancement."""
    
    def __init__(self):
        """Initialize the enhanced dependency detector."""
        # Common CQL patterns that indicate dependencies
        self.dependency_patterns = [
            # Direct define references: "Office Visits", 'Emergency Visits'
            (r'"([^"]+)"', 'direct_quote'),
            (r"'([^']+)'", 'direct_single_quote'),
            
            # Function calls with define references: exists "Some Define"
            (r'exists\s+["\']([^"\']+)["\']', 'exists_reference'),
            
            # Union/intersect operations: "Define A" union "Define B"  
            (r'["\']([^"\']+)["\']\s+union\s+["\']([^"\']+)["\']', 'union_operation'),
            (r'["\']([^"\']+)["\']\s+intersect\s+["\']([^"\']+)["\']', 'intersect_operation'),
            
            # With/such that clauses: "Define A" with "Define B" such that...
            (r'["\']([^"\']+)["\']\s+with\s+["\']([^"\']+)["\']', 'with_clause'),
        ]
    
    def detect_dependencies(self, cql_expression: str, available_defines: Set[str]) -> List[str]:
        """
        Detect dependencies from a single CQL expression.
        
        Args:
            cql_expression: CQL expression to analyze
            available_defines: Set of available define names
            
        Returns:
            List of define names this expression depends on
        """
        dependencies = set()
        cql_clean = cql_expression.strip()
        
        # Apply dependency detection patterns
        for pattern, pattern_type in self.dependency_patterns:
            matches = re.findall(pattern, cql_clean, re.IGNORECASE)
            
            if pattern_type in ['union_operation', 'intersect_operation', 'with_clause']:
                # These patterns return tuples for multiple captures
                for match_tuple in matches:
                    if isinstance(match_tuple, tuple):
                        for match in match_tuple:
                            if match in available_defines:
                                dependencies.add(match)
            else:
                # Single capture patterns
                for match in matches:
                    if match in available_defines:
                        dependencies.add(match)
        
        return list(dependencies)


class ResourceTypeDetector:
    """
    Enhanced resource type detector using existing patterns from functions.py.
    
    This builds on the proven resource type detection logic while extending
    it for CTE-specific requirements.
    """
    
    # Resource type patterns with improved detection
    RESOURCE_TYPE_PATTERNS = {
        'Patient': {
            'resource_syntax': ['[Patient]', '[Patient:'],
            'indicators': ['patient', 'AgeInYears()', 'birthDate', 'gender'],
            'priority': 10
        },
        'Condition': {
            'resource_syntax': ['[Condition]', '[Condition:'],
            'indicators': ['asthma', 'diabetes', 'hypertension', 'diagnosis'],
            'priority': 8
        },
        'MedicationDispense': {
            'resource_syntax': ['[MedicationDispense]', '[MedicationDispense:'],
            'indicators': ['medication', 'dispense', 'medicationCodeableConcept'],
            'priority': 7
        },
        'Encounter': {
            'resource_syntax': ['[Encounter]', '[Encounter:'],
            'indicators': ['visit', 'hospitalization', 'admission'],
            'priority': 6
        },
        'Observation': {
            'resource_syntax': ['[Observation]', '[Observation:'],
            'indicators': ['observation', 'vital', 'lab', 'measurement'],
            'priority': 5
        },
        'Procedure': {
            'resource_syntax': ['[Procedure]', '[Procedure:'],
            'indicators': ['procedure', 'surgery', 'operation'],
            'priority': 4
        },
        'DiagnosticReport': {
            'resource_syntax': ['[DiagnosticReport]', '[DiagnosticReport:'],
            'indicators': ['diagnostic', 'report', 'result'],
            'priority': 3
        },
        'Immunization': {
            'resource_syntax': ['[Immunization]', '[Immunization:'],
            'indicators': ['immunization', 'vaccine', 'vaccination'],
            'priority': 2
        }
    }
    
    def detect_from_cql(self, cql_expr: str, define_name: str = "") -> str:
        """
        Detect FHIR resource type from CQL expression.

        Uses improved detection logic that prioritizes explicit FHIR resource syntax
        over contextual indicators.

        Args:
            cql_expr: CQL expression to analyze
            define_name: Define name for additional context

        Returns:
            Detected FHIR resource type
        """
        cql_lower = cql_expr.lower()
        define_lower = define_name.lower()

        # First pass: Check for explicit FHIR resource syntax (highest priority)
        resource_syntax_matches = {}
        for resource_type, config in self.RESOURCE_TYPE_PATTERNS.items():
            for syntax in config['resource_syntax']:
                if syntax.lower() in cql_lower:
                    # Give highest priority to explicit resource syntax
                    resource_syntax_matches[resource_type] = resource_syntax_matches.get(resource_type, 0) + 50

        if resource_syntax_matches:
            # If we found explicit resource syntax, prefer it
            detected_type = max(resource_syntax_matches.items(), key=lambda x: x[1])[0]
            logger.debug(f"Detected resource type '{detected_type}' from explicit syntax in CQL: '{cql_expr[:50]}...'")
            return detected_type

        # Second pass: Score based on contextual indicators
        scores = {}
        for resource_type, config in self.RESOURCE_TYPE_PATTERNS.items():
            score = 0

            # Check CQL expression indicators
            for indicator in config['indicators']:
                if indicator.lower() in cql_lower:
                    score += config['priority']

            # Check define name indicators
            for indicator in config['indicators']:
                if indicator.lower() in define_lower:
                    score += config['priority'] // 2

            if score > 0:
                scores[resource_type] = score

        if scores:
            # Return resource type with highest score
            detected_type = max(scores.items(), key=lambda x: x[1])[0]
            logger.debug(f"Detected resource type '{detected_type}' from indicators in CQL: '{cql_expr[:50]}...' (scores: {scores})")
            return detected_type

        # Enhanced stack analysis fallback (from existing functions.py)
        detected_from_stack = self._detect_from_call_stack(define_name)
        if detected_from_stack != 'Patient':
            return detected_from_stack

        # Default fallback
        logger.debug(f"Using default Patient resource type for CQL: '{cql_expr[:50]}...'")
        return 'Patient'
    
    def _detect_from_call_stack(self, define_name: str) -> str:
        """
        Use call stack analysis to find define name patterns.
        
        This replicates the enhanced stack analysis from existing functions.py
        to maintain consistency with current resource type detection.
        """
        import inspect
        
        try:
            define_lower = define_name.lower()
            
            # Check define name patterns directly
            if any(term in define_lower for term in ['asthma', 'copd', 'diabetes', 'hypertension']):
                return 'Condition'
            elif any(term in define_lower for term in ['medication', 'dispense', 'drug', 'therapy']):
                return 'MedicationDispense' 
            elif any(term in define_lower for term in ['encounter', 'visit', 'admission']):
                return 'Encounter'
            elif any(term in define_lower for term in ['observation', 'vital', 'lab']):
                return 'Observation'
            
            # Look through call stack for additional context
            for frame_info in inspect.stack():
                frame_locals = frame_info.frame.f_locals
                
                # Check local variables for resource type hints
                for var_name, var_value in frame_locals.items():
                    if isinstance(var_value, str):
                        var_lower = var_value.lower()
                        if any(term in var_lower for term in ['condition', 'diagnosis']):
                            return 'Condition'
                        elif any(term in var_lower for term in ['medication', 'drug']):
                            return 'MedicationDispense'
                        elif any(term in var_lower for term in ['encounter', 'visit']):
                            return 'Encounter'
        
        except Exception as e:
            logger.debug(f"Stack analysis failed: {e}")
        
        return 'Patient'  # Safe default


class CQLPatternAnalyzer:
    """
    Analyzes CQL expressions to extract filter conditions and patterns.
    
    This component handles the parsing of CQL expressions to extract
    conditions that can be translated into SQL WHERE clauses for CTEs.
    """
    
    def __init__(self, dialect: str):
        """Initialize pattern analyzer for specific database dialect."""
        self.dialect = dialect.upper()
    
    def extract_filter_conditions(self, cql_expr: str, resource_type: str) -> List[str]:
        """
        Extract WHERE conditions from CQL expression.
        
        Converts CQL filter expressions into SQL conditions using existing
        patterns from pipeline operations and maintaining dialect compatibility.
        
        Args:
            cql_expr: CQL expression to analyze
            resource_type: FHIR resource type being queried
            
        Returns:
            List of SQL WHERE conditions
        """
        conditions = []
        
        # Always add resource type filter (existing pattern)
        resource_type_condition = self._get_resource_type_condition(resource_type)
        conditions.append(resource_type_condition)
        
        # Extract age filters (from existing age calculation patterns)
        age_conditions = self._extract_age_conditions(cql_expr)
        conditions.extend(age_conditions)
        
        # Extract code/text filters
        code_conditions = self._extract_code_conditions(cql_expr, resource_type)
        conditions.extend(code_conditions)
        
        # Extract date range filters
        date_conditions = self._extract_date_conditions(cql_expr)
        conditions.extend(date_conditions)
        
        # Extract boolean filters
        boolean_conditions = self._extract_boolean_conditions(cql_expr)
        conditions.extend(boolean_conditions)
        
        logger.debug(f"Extracted {len(conditions)} conditions from CQL: {cql_expr[:50]}...")
        return conditions
    
    def _get_resource_type_condition(self, resource_type: str) -> str:
        """Generate resource type filter condition."""
        if self.dialect == 'DUCKDB':
            return f"json_extract_string(resource, '$.resourceType') = '{resource_type}'"
        else:  # PostgreSQL
            return f"jsonb_extract_path_text(resource, 'resourceType') = '{resource_type}'"
    
    def _extract_age_conditions(self, cql_expr: str) -> List[str]:
        """
        Extract age-related conditions from CQL.
        
        Handles patterns like:
        - AgeInYears() >= 18
        - AgeInYears() between 5 and 64
        """
        conditions = []
        
        # Pattern for age comparisons
        age_patterns = [
            r'AgeInYears\(\)\s*([><=!]+)\s*(\d+)',
            r'AgeInYears\(\)\s*between\s*(\d+)\s*and\s*(\d+)',
        ]
        
        # Age calculation SQL (existing pattern from population optimization)
        if self.dialect == 'DUCKDB':
            age_sql = "EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM CAST(json_extract_string(resource, '$.birthDate') AS DATE))"
        else:  # PostgreSQL
            age_sql = "EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM (jsonb_extract_path_text(resource, 'birthDate'))::DATE)"
        
        for pattern in age_patterns:
            matches = re.finditer(pattern, cql_expr, re.IGNORECASE)
            for match in matches:
                if 'between' in match.group().lower():
                    min_age, max_age = match.groups()
                    conditions.append(f"({age_sql}) BETWEEN {min_age} AND {max_age}")
                else:
                    operator, value = match.groups()
                    conditions.append(f"({age_sql}) {operator} {value}")
        
        return conditions
    
    def _extract_code_conditions(self, cql_expr: str, resource_type: str) -> List[str]:
        """
        Extract code/terminology conditions from CQL.
        
        Handles patterns like:
        - [Condition: "Asthma"]
        - code contains "asthma"
        - code.text = "Diabetes"
        """
        conditions = []
        
        # Extract terminology codes from brackets
        bracket_pattern = r'\[' + re.escape(resource_type) + r':\s*["\']([^"\']+)["\']'
        matches = re.finditer(bracket_pattern, cql_expr, re.IGNORECASE)
        
        for match in matches:
            code_text = match.group(1)
            
            if self.dialect == 'DUCKDB':
                # Check both code.text and code.coding arrays
                conditions.extend([
                    f"json_extract_string(resource, '$.code.text') LIKE '%{code_text}%'",
                    f"EXISTS (SELECT 1 FROM json_each(json_extract(resource, '$.code.coding')) AS coding WHERE json_extract_string(coding.value, '$.display') LIKE '%{code_text}%')"
                ])
            else:  # PostgreSQL
                conditions.extend([
                    f"jsonb_extract_path_text(resource, 'code', 'text') LIKE '%{code_text}%'",
                    f"EXISTS (SELECT 1 FROM jsonb_array_elements(jsonb_extract_path(resource, 'code', 'coding')) AS coding WHERE coding ->> 'display' LIKE '%{code_text}%')"
                ])
        
        # Extract "where" conditions with text matching
        where_patterns = [
            r'where\s+code\.text\s+contains?\s+["\']([^"\']+)["\']',
            r'where\s+code\s+contains?\s+["\']([^"\']+)["\']'
        ]
        
        for pattern in where_patterns:
            matches = re.finditer(pattern, cql_expr, re.IGNORECASE)
            for match in matches:
                search_text = match.group(1)
                
                if self.dialect == 'DUCKDB':
                    conditions.append(f"json_extract_string(resource, '$.code.text') LIKE '%{search_text}%'")
                else:  # PostgreSQL
                    conditions.append(f"jsonb_extract_path_text(resource, 'code', 'text') LIKE '%{search_text}%'")
        
        return conditions
    
    def _extract_date_conditions(self, cql_expr: str) -> List[str]:
        """Extract date-related conditions from CQL."""
        conditions = []
        
        # Pattern for date comparisons
        date_patterns = [
            r'effectiveDateTime\s*([><=!]+)\s*["\']([^"\']+)["\']',
            r'period\.start\s*([><=!]+)\s*["\']([^"\']+)["\']'
        ]
        
        for pattern in date_patterns:
            matches = re.finditer(pattern, cql_expr, re.IGNORECASE)
            for match in matches:
                operator, date_value = match.groups()
                
                if self.dialect == 'DUCKDB':
                    conditions.append(f"json_extract_string(resource, '$.effectiveDateTime') {operator} '{date_value}'")
                else:  # PostgreSQL
                    conditions.append(f"jsonb_extract_path_text(resource, 'effectiveDateTime') {operator} '{date_value}'")
        
        return conditions
    
    def _extract_boolean_conditions(self, cql_expr: str) -> List[str]:
        """Extract boolean field conditions from CQL."""
        conditions = []
        
        # Pattern for boolean field checks
        boolean_patterns = [
            r'active\s*=\s*(true|false)',
            r'deceased\s*=\s*(true|false)',
        ]
        
        for pattern in boolean_patterns:
            matches = re.finditer(pattern, cql_expr, re.IGNORECASE)
            for match in matches:
                field_value = match.group(1).lower()
                
                if 'active' in match.group():
                    if self.dialect == 'DUCKDB':
                        conditions.append(f"json_extract_string(resource, '$.active') = '{field_value}'")
                    else:  # PostgreSQL
                        conditions.append(f"jsonb_extract_path_text(resource, 'active') = '{field_value}'")
        
        return conditions


class CQLToCTEConverter:
    """
    Converts CQL expressions to CTE fragments.
    
    This is the main converter that builds on existing CQL processing
    infrastructure while providing CTE-specific functionality for the
    monolithic query approach.
    
    Builds on existing CQLToPipelineConverter infrastructure and integrates
    with terminology services, resource type detection, and dialect patterns.
    """
    
    def __init__(self, dialect: str, terminology_client=None, datastore=None):
        """
        Initialize CQL to CTE converter.

        Args:
            dialect: Database dialect ('duckdb' or 'postgresql')
            terminology_client: Optional terminology client for value set resolution
            datastore: Optional datastore for ValueSet caching
        """
        self.dialect = dialect.upper()
        self.terminology_client = terminology_client
        self.datastore = datastore
        self.valueset_mappings = {}  # Maps valueset names to OIDs
        self.resource_type_detector = ResourceTypeDetector()
        self.pattern_analyzer = CQLPatternAnalyzer(dialect)
        
        # Task 3.2 Enhancement: Enhanced dependency detection
        self.dependency_detector = EnhancedDependencyDetector()
        
        # ValueSet CTE tracking
        self.required_valuesets = {}  # Maps ValueSet names to expansion data
        self.valueset_cte_names = {}  # Maps ValueSet names to CTE names

        # Conversion statistics for monitoring
        self.conversion_stats = {
            'expressions_converted': 0,
            'resource_types_detected': {},
            'patterns_extracted': 0,
            'terminology_resolutions': 0,
            'dependencies_detected': 0  # Task 3.2: track dependency detection
        }
        
        logger.debug(f"Initialized CQL to CTE converter for {self.dialect} dialect")
    
    def convert_cql_expression(self, define_name: str, cql_expr: str) -> CTEFragment:
        """
        Convert single CQL define to CTE fragment.

        This is the main conversion method that processes CQL expressions
        and generates CTE fragments that can be combined into monolithic queries.

        Args:
            define_name: Name of the CQL define
            cql_expr: CQL expression to convert

        Returns:
            CTEFragment representing the CQL define as a CTE
        """
        logger.debug(f"Converting CQL define '{define_name}': {cql_expr}")

        # Check if this is a query expression that needs special handling
        if self._is_query_expression(cql_expr):
            return self._convert_query_expression(define_name, cql_expr)

        # Detect resource type using enhanced detection
        resource_type = self.resource_type_detector.detect_from_cql(cql_expr, define_name)

        # Update statistics
        self.conversion_stats['expressions_converted'] += 1
        resource_counts = self.conversion_stats['resource_types_detected']
        resource_counts[resource_type] = resource_counts.get(resource_type, 0) + 1
        
        # Generate CTE name from define name
        cte_name = self._normalize_define_name_to_cte(define_name)
        
        # Extract filter conditions using pattern analysis
        where_conditions = self.pattern_analyzer.extract_filter_conditions(cql_expr, resource_type)
        self.conversion_stats['patterns_extracted'] += len(where_conditions)
        
        # Resolve terminology if present
        terminology_conditions = []
        if self.terminology_client and self._has_terminology(cql_expr):
            terminology_conditions = self._resolve_terminology_conditions(cql_expr, resource_type)
            self.conversion_stats['terminology_resolutions'] += len(terminology_conditions)
        
        # Combine conditions - if terminology resolution succeeded, prefer it over text matching
        if terminology_conditions:
            # Filter out text matching conditions from pattern analyzer
            filtered_pattern_conditions = []
            for condition in where_conditions:
                # Keep resource type and structural conditions, remove text matching
                if (not 'LIKE' in condition or
                    'resourceType' in condition):
                    filtered_pattern_conditions.append(condition)
            all_conditions = filtered_pattern_conditions + terminology_conditions
        else:
            # No terminology resolution, use pattern conditions as fallback
            all_conditions = where_conditions
        
        # Build select fields for the CTE
        select_fields = self._build_select_fields(resource_type, cql_expr)
        
        # Create the CTE fragment
        fragment = CTEFragment(
            name=cte_name,
            resource_type=resource_type,
            patient_id_extraction="",  # Will be set by _build_patient_id_extraction
            select_fields=select_fields,
            from_clause="fhir_resources",
            where_conditions=all_conditions,
            source_cql_expression=cql_expr,
            define_name=define_name,
            result_type="boolean"  # Most CQL defines return boolean results
        )
        
        # Set patient ID extraction using fragment's method
        patient_id_sql = fragment.get_patient_id_sql(self.dialect)
        object.__setattr__(fragment, 'patient_id_extraction', patient_id_sql)
        
        logger.info(f"Converted CQL define '{define_name}' to CTE '{cte_name}' "
                   f"({resource_type}, {len(all_conditions)} conditions)")
        
        return fragment
    
    def convert_multiple_defines(self, define_statements: Dict[str, str]) -> Dict[str, CTEFragment]:
        """
        Convert multiple CQL defines to CTE fragments with enhanced dependency detection.
        
        Task 3.2 Enhancement: Automatic dependency detection and resolution.
        
        Args:
            define_statements: Dictionary mapping define names to CQL expressions
            
        Returns:
            Dictionary mapping define names to CTE fragments
        """
        fragments = {}
        available_defines = set(define_statements.keys())
        
        logger.info(f"Converting {len(define_statements)} CQL defines to CTE fragments with enhanced dependency detection")
        
        for define_name, cql_expr in define_statements.items():
            try:
                fragment = self.convert_cql_expression(define_name, cql_expr)
                
                # Task 3.2 Enhancement: Detect and set dependencies automatically
                detected_deps = self.dependency_detector.detect_dependencies(cql_expr, available_defines)
                if detected_deps:
                    # Update fragment dependencies (need to convert define names to CTE names)
                    cte_deps = [dep.lower().replace(' ', '_').replace('-', '_') for dep in detected_deps]
                    # Create new fragment with detected dependencies
                    fragment = CTEFragment(
                        name=fragment.name,
                        resource_type=fragment.resource_type,
                        patient_id_extraction=fragment.patient_id_extraction,
                        select_fields=fragment.select_fields,
                        from_clause=fragment.from_clause,
                        where_conditions=fragment.where_conditions,
                        dependencies=cte_deps,  # Enhanced dependencies
                        source_cql_expression=fragment.source_cql_expression,
                        complexity_score=fragment.complexity_score,
                        define_name=fragment.define_name,
                        result_type=fragment.result_type
                    )
                    
                    self.conversion_stats['dependencies_detected'] += len(detected_deps)
                    logger.debug(f"Detected dependencies for '{define_name}': {detected_deps}")
                
                fragments[define_name] = fragment
            except Exception as e:
                logger.error(f"Failed to convert define '{define_name}': {e}")
                # Create a fallback fragment
                fragments[define_name] = self._create_fallback_fragment(define_name, cql_expr)
        
        return fragments
    
    def _normalize_define_name_to_cte(self, define_name: str) -> str:
        """
        Normalize define name to valid CTE identifier.
        
        Uses the same normalization logic as CTEQueryBuilder for consistency.
        """
        # Convert to lowercase and replace spaces/hyphens with underscores
        normalized = define_name.lower().replace(' ', '_').replace('-', '_')
        
        # Remove non-alphanumeric characters except underscores
        normalized = ''.join(c for c in normalized if c.isalnum() or c == '_')
        
        # Ensure it starts with a letter or underscore
        if normalized and normalized[0].isdigit():
            normalized = f"cte_{normalized}"
        
        return normalized or "unnamed_cte"
    
    def _build_select_fields(self, resource_type: str, cql_expr: str) -> List[str]:
        """
        Build SELECT fields for the CTE based on resource type and CQL expression.
        
        Args:
            resource_type: FHIR resource type
            cql_expr: Original CQL expression
            
        Returns:
            List of SELECT field expressions
        """
        select_fields = []
        
        # Always include patient ID
        if resource_type == 'Patient':
            if self.dialect == 'DUCKDB':
                patient_id_sql = "json_extract_string(resource, '$.id') as patient_id"
            else:  # PostgreSQL
                patient_id_sql = "jsonb_extract_path_text(resource, 'id') as patient_id"
        else:
            # Subject reference extraction
            if self.dialect == 'DUCKDB':
                patient_id_sql = "REPLACE(json_extract_string(resource, '$.subject.reference'), 'Patient/', '') as patient_id"
            else:  # PostgreSQL
                patient_id_sql = "REPLACE(jsonb_extract_path_text(resource, 'subject', 'reference'), 'Patient/', '') as patient_id"
        
        select_fields.append(patient_id_sql)
        
        # Add result field (boolean for most defines)
        select_fields.append("true as result")
        
        # Add resource ID for debugging/tracing
        if self.dialect == 'DUCKDB':
            select_fields.append("json_extract_string(resource, '$.id') as resource_id")
        else:  # PostgreSQL
            select_fields.append("jsonb_extract_path_text(resource, 'id') as resource_id")
        
        return select_fields
    
    def _has_terminology(self, cql_expr: str) -> bool:
        """Check if CQL expression contains terminology references."""
        # Look for value set patterns
        terminology_patterns = [
            r'\[[^\]]+:\s*["\'][^"\']+["\']\]',  # [Condition: "Asthma"]
            r'in\s+["\'][^"\']+["\']',           # in "ValueSetOID"
            r'code\s+in\s+',                     # code in ValueSet
        ]
        
        for pattern in terminology_patterns:
            if re.search(pattern, cql_expr, re.IGNORECASE):
                return True
        
        return False
    
    def _resolve_terminology_conditions(self, cql_expr: str, resource_type: str) -> List[str]:
        """
        Resolve terminology/value set conditions.
        
        This integrates with existing terminology services and VSAC client
        to expand value sets into concrete code conditions.
        
        Args:
            cql_expr: CQL expression containing terminology
            resource_type: FHIR resource type
            
        Returns:
            List of SQL conditions for terminology filtering
        """
        conditions = []
        
        if not self.terminology_client:
            logger.debug("No terminology client available, using text matching fallback")
            return self._generate_text_matching_conditions(cql_expr, resource_type)
        
        try:
            # Extract value set references from CQL
            value_set_refs = self._extract_value_set_references(cql_expr)
            
            for value_set_ref in value_set_refs:
                # First, check datastore cache for ValueSet
                cached_valueset = None
                if self.datastore:
                    cached_valueset = self.datastore.get_cached_valueset(value_set_ref)
                    if cached_valueset:
                        logger.debug(f"Using cached ValueSet: '{value_set_ref}'")

                if cached_valueset:
                    # Use cached ValueSet expansion
                    valueset_expansion = cached_valueset
                else:
                    # Fall back to VSAC call
                    valueset_oid = self.valueset_mappings.get(value_set_ref)
                    if not valueset_oid:
                        logger.warning(f"No OID mapping found for ValueSet: '{value_set_ref}', falling back to text matching")
                        conditions.extend(self._generate_text_matching_conditions(cql_expr, resource_type))
                        continue

                    logger.debug(f"Converting ValueSet '{value_set_ref}' to OID '{valueset_oid}' for VSAC call")

                    # Expand value set using OID (not name)
                    valueset_expansion = self.terminology_client.expand_valueset(valueset_oid)

                    # Cache the VSAC response for future use
                    if valueset_expansion and self.datastore:
                        try:
                            self.datastore.cache_valueset(value_set_ref, valueset_expansion)
                            logger.debug(f"Cached VSAC response for ValueSet: '{value_set_ref}'")
                        except Exception as e:
                            logger.warning(f"Failed to cache ValueSet '{value_set_ref}': {e}")
                
                if valueset_expansion:
                    # Store ValueSet as regular FHIR resource in datastore
                    try:
                        self._store_valueset_as_fhir_resource(value_set_ref, valueset_expansion)
                        logger.debug(f"Stored ValueSet '{value_set_ref}' as FHIR resource")

                        # Generate FHIRPath-based query conditions that reference the ValueSet resource
                        valueset_id = self._generate_valueset_resource_id(value_set_ref)
                        conditions.extend(self._generate_valueset_fhir_conditions(valueset_id, resource_type))

                    except Exception as e:
                        logger.warning(f"Failed to store ValueSet '{value_set_ref}' as FHIR resource: {e}")
                        # Fall back to text matching if resource storage fails
                        conditions.extend(self._generate_text_matching_conditions(cql_expr, resource_type))
                
                else:
                    logger.warning(f"VSAC expansion returned empty for ValueSet OID: {valueset_oid} (name: {value_set_ref})")
        
        except Exception as e:
            logger.warning(f"Terminology resolution failed: {e}")
            conditions.extend(self._generate_text_matching_conditions(cql_expr, resource_type))
        
        return conditions
    
    def _extract_value_set_references(self, cql_expr: str) -> List[str]:
        """Extract value set references from CQL expression and resolve names to OIDs."""
        value_set_refs = []
        
        # Pattern for value set in brackets: [Condition: "ValueSetName"]
        bracket_pattern = r'\[[^\]]+:\s*["\']([^"\']+)["\']\]'
        matches = re.finditer(bracket_pattern, cql_expr, re.IGNORECASE)
        
        for match in matches:
            valueset_name = match.group(1)
            # Return the name for terminology client expansion
            value_set_refs.append(valueset_name)
            logger.debug(f"Extracted valueset name: '{valueset_name}'")
        
        # Pattern for explicit value set references: in "ValueSetOID"
        in_pattern = r'in\s+["\']([^"\']+)["\']'
        matches = re.finditer(in_pattern, cql_expr, re.IGNORECASE)
        
        for match in matches:
            value_set_ref = match.group(1)
            # Return the reference for terminology client expansion (can be name or OID)
            value_set_refs.append(value_set_ref)
            logger.debug(f"Extracted valueset reference: '{value_set_ref}'")
        
        return value_set_refs
    
    def _extract_codes_from_expansion(self, valueset_expansion: Dict[str, Any]) -> List[str]:
        """
        Extract codes from FHIR ValueSet expansion.
        
        Args:
            valueset_expansion: FHIR ValueSet resource with expansion
            
        Returns:
            List of codes from the expansion
        """
        codes = []
        
        try:
            expansion = valueset_expansion.get('expansion', {})
            contains = expansion.get('contains', [])
            
            for entry in contains:
                code = entry.get('code')
                if code:
                    codes.append(code)
                    
            logger.debug(f"Extracted {len(codes)} codes from ValueSet expansion")
            
        except Exception as e:
            logger.warning(f"Failed to extract codes from ValueSet expansion: {e}")
            
        return codes
    
    def set_valueset_mappings(self, library_content: str):
        """
        Parse valueset definitions from CQL library and build name-to-OID mappings.
        
        Args:
            library_content: Complete CQL library text
        """
        self.valueset_mappings = {}
        
        # Extract valueset definitions: valueset "Name": 'OID'
        import re
        valueset_pattern = r'valueset\s+"([^"]+)"\s*:\s*[\'"]([^\'"]+)[\'"]'
        matches = re.finditer(valueset_pattern, library_content, re.IGNORECASE | re.MULTILINE)
        
        for match in matches:
            name = match.group(1)
            oid = match.group(2)
            self.valueset_mappings[name] = oid
            logger.debug(f"Found valueset mapping: '{name}' -> '{oid}'")
        
        logger.info(f"Loaded {len(self.valueset_mappings)} valueset mappings")
    
    def _generate_text_matching_conditions(self, cql_expr: str, resource_type: str) -> List[str]:
        """
        Generate text matching conditions as fallback when terminology expansion fails.
        
        This provides a fallback mechanism when VSAC or other terminology
        services are not available or fail to expand value sets.
        """
        conditions = []
        
        # Extract text patterns to match against
        text_patterns = []
        
        # From bracket notation: [Condition: "Asthma"]
        bracket_matches = re.finditer(r'\[[^\]]+:\s*["\']([^"\']+)["\']\]', cql_expr, re.IGNORECASE)
        text_patterns.extend(match.group(1) for match in bracket_matches)
        
        # From contains expressions: code.text contains "diabetes"
        contains_matches = re.finditer(r'contains?\s+["\']([^"\']+)["\']', cql_expr, re.IGNORECASE)
        text_patterns.extend(match.group(1) for match in contains_matches)
        
        # Generate SQL conditions for text matching
        for text_pattern in text_patterns:
            if self.dialect == 'DUCKDB':
                conditions.extend([
                    f"json_extract_string(resource, '$.code.text') LIKE '%{text_pattern}%'",
                    f"EXISTS (SELECT 1 FROM json_each(json_extract(resource, '$.code.coding')) AS coding WHERE json_extract_string(coding.value, '$.display') LIKE '%{text_pattern}%')"
                ])
            else:  # PostgreSQL
                conditions.extend([
                    f"jsonb_extract_path_text(resource, 'code', 'text') LIKE '%{text_pattern}%'",
                    f"EXISTS (SELECT 1 FROM jsonb_array_elements(jsonb_extract_path(resource, 'code', 'coding')) AS coding WHERE coding ->> 'display' LIKE '%{text_pattern}%')"
                ])
        
        logger.debug(f"Generated {len(conditions)} text matching conditions")
        return conditions
    
    def _create_fallback_fragment(self, define_name: str, cql_expr: str) -> CTEFragment:
        """
        Create a fallback CTE fragment when conversion fails.
        
        This ensures that the monolithic query can still be generated
        even if some define statements cannot be properly converted.
        """
        logger.warning(f"Creating fallback fragment for define '{define_name}'")
        
        cte_name = self._normalize_define_name_to_cte(define_name)
        
        # Simple fallback: return all patients with NULL result
        if self.dialect == 'DUCKDB':
            select_fields = [
                "json_extract_string(resource, '$.id') as patient_id",
                "NULL as result",
                "json_extract_string(resource, '$.id') as resource_id"
            ]
            where_conditions = ["json_extract_string(resource, '$.resourceType') = 'Patient'"]
        else:  # PostgreSQL
            select_fields = [
                "jsonb_extract_path_text(resource, 'id') as patient_id",
                "NULL as result", 
                "jsonb_extract_path_text(resource, 'id') as resource_id"
            ]
            where_conditions = ["jsonb_extract_path_text(resource, 'resourceType') = 'Patient'"]
        
        return CTEFragment(
            name=cte_name,
            resource_type="Patient",
            patient_id_extraction=select_fields[0],
            select_fields=select_fields,
            from_clause="fhir_resources",
            where_conditions=where_conditions,
            source_cql_expression=cql_expr,
            define_name=define_name,
            result_type="fallback"
        )
    
    # ValueSet CTE Helper Methods

    def _generate_valueset_cte_name(self, valueset_name: str) -> str:
        """
        Generate valid SQL identifier for ValueSet CTE name.

        Args:
            valueset_name: Original ValueSet name

        Returns:
            Sanitized CTE name safe for SQL
        """
        import re
        # Convert to lowercase and replace non-alphanumeric with underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', valueset_name.lower())

        # Ensure it starts with a letter
        if not sanitized[0].isalpha():
            sanitized = f"vs_{sanitized}"

        # Add suffix to indicate it's a ValueSet CTE
        return f"{sanitized}_vs"

    def _track_required_valueset(self, valueset_name: str, valueset_expansion: Dict[str, Any], cte_name: str):
        """
        Track ValueSet for CTE generation.

        Args:
            valueset_name: Name of the ValueSet
            valueset_expansion: FHIR ValueSet resource with expansion
            cte_name: Generated CTE name
        """
        self.required_valuesets[valueset_name] = valueset_expansion
        self.valueset_cte_names[valueset_name] = cte_name
        logger.debug(f"Tracked ValueSet '{valueset_name}' for CTE generation as '{cte_name}'")

    def get_required_valuesets(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all ValueSets required for CTE generation.

        Returns:
            Dictionary mapping ValueSet names to their expansion data
        """
        return dict(self.required_valuesets)

    def get_valueset_cte_names(self) -> Dict[str, str]:
        """
        Get mapping of ValueSet names to their CTE names.

        Returns:
            Dictionary mapping ValueSet names to CTE names
        """
        return dict(self.valueset_cte_names)

    def clear_valueset_tracking(self):
        """Clear ValueSet tracking for fresh processing."""
        self.required_valuesets.clear()
        self.valueset_cte_names.clear()

    def _store_valueset_as_fhir_resource(self, valueset_name: str, valueset_expansion: Dict[str, Any]):
        """
        Store ValueSet expansion as a FHIR resource in the datastore.

        Args:
            valueset_name: Name of the ValueSet
            valueset_expansion: FHIR ValueSet resource with expansion
        """
        if not self.datastore:
            raise ValueError("No datastore available for ValueSet storage")

        # Generate a consistent resource ID for the ValueSet
        resource_id = self._generate_valueset_resource_id(valueset_name)

        # Ensure the ValueSet has a proper ID
        valueset_resource = dict(valueset_expansion)
        valueset_resource['id'] = resource_id

        # Store as FHIR resource in datastore
        self.datastore.load_resource(valueset_resource)
        logger.debug(f"Stored ValueSet '{valueset_name}' as FHIR resource with ID '{resource_id}'")

    def _generate_valueset_resource_id(self, valueset_name: str) -> str:
        """
        Generate a consistent FHIR resource ID for a ValueSet.

        Args:
            valueset_name: Name of the ValueSet

        Returns:
            FHIR resource ID
        """
        # Create a safe resource ID from the valueset name
        # Replace spaces and special characters with underscores
        import re
        safe_id = re.sub(r'[^a-zA-Z0-9]', '_', valueset_name.lower())
        return f"valueset-{safe_id}"

    def _generate_valueset_fhir_conditions(self, valueset_id: str, resource_type: str) -> List[str]:
        """
        Generate SQL conditions that query ValueSet codes from FHIR resources.
        Matches both code and system from the ValueSet expansion using dialect-specific SQL.

        Args:
            valueset_id: ID of the ValueSet FHIR resource
            resource_type: FHIR resource type being filtered

        Returns:
            List of SQL conditions
        """
        # Get the dialect-specific SQL from the appropriate dialect implementation
        if hasattr(self.datastore, 'dialect') and self.datastore.dialect:
            dialect_condition = self.datastore.dialect.generate_valueset_match_condition(valueset_id)
            return [dialect_condition]
        else:
            # Fallback - should not happen in normal operation
            logger.warning(f"No dialect available for ValueSet matching, falling back to basic implementation")
            return [f"/* ValueSet matching not available - no dialect */"]

    def get_conversion_statistics(self) -> Dict[str, Any]:
        """Get statistics about CQL conversions performed."""
        return dict(self.conversion_stats)
    
    def reset_statistics(self) -> None:
        """Reset conversion statistics."""
        self.conversion_stats = {
            'expressions_converted': 0,
            'resource_types_detected': {},
            'patterns_extracted': 0,
            'terminology_resolutions': 0
        }

    # =================================================================================
    # Query Expression Handling (Step 1.3: Enhanced CQL Query Expression SQL Generation)
    # =================================================================================

    def _is_query_expression(self, cql_expr: str) -> bool:
        """
        Detect if CQL expression is a query expression that needs special handling.

        Query expressions include patterns like:
        - from ({2, 3}) A, ({5, 6}) B
        - ({1, 2, 3}) l sort desc
        - ({1, 2, 3, 3, 4}) L aggregate A starting 1: A * L
        - (4) l return 'Hello World'
        - [Resource] R with [Other] O such that condition (relationship expressions)
        """
        import re

        # Multi-source from queries
        if re.search(r'\bfrom\s+\([^)]+\)\s+\w+\s*,\s*\([^)]+\)\s+\w+', cql_expr, re.IGNORECASE):
            return True

        # Aliased collection queries with clauses
        if re.search(r'\([^)]+\)\s+[a-zA-Z][a-zA-Z0-9]*\s+(sort|return|aggregate)', cql_expr, re.IGNORECASE):
            return True

        # Simple aliased queries (like "(4) l")
        if re.search(r'^\s*\([^)]+\)\s+[a-zA-Z][a-zA-Z0-9]*\s*$', cql_expr.strip(), re.IGNORECASE):
            return True

        # Relationship expressions with with/without clauses
        if re.search(r'\[[^\]]+\]\s+\w+\s+(with|without)\s+\[[^\]]+\]', cql_expr, re.IGNORECASE):
            return True

        # Malformed relationship expressions (for error handling)
        if re.search(r'\b(with|without)\s+such\s+that', cql_expr, re.IGNORECASE):
            return True

        # Collection operations (union, intersect, except, flatten, distinct)
        if re.search(r'\b(union|intersect|except)\b', cql_expr, re.IGNORECASE):
            return True

        # Collection function calls (flatten(), distinct(), etc.)
        if re.search(r'\.(flatten|distinct|sort|group|partition|reduce)\s*\(', cql_expr, re.IGNORECASE):
            return True

        # Predicate-based collection operations (unionBy, distinctBy, etc.)
        if re.search(r'\.(unionby|intersectby|exceptby|distinctby|sortby)\s*\(', cql_expr, re.IGNORECASE):
            return True

        return False

    def _convert_query_expression(self, define_name: str, cql_expr: str) -> CTEFragment:
        """
        Convert query expression to CTE fragment using the advanced parser.

        Args:
            define_name: Name of the CQL define
            cql_expr: CQL query expression to convert

        Returns:
            CTEFragment representing the query expression as a CTE
        """
        logger.debug(f"Converting query expression '{define_name}': {cql_expr}")

        try:
            # Import and use the advanced parser
            from ...cql.core.advanced_parser import AdvancedCQLParser
            parser = AdvancedCQLParser()

            # Parse the query expression
            query_node = parser.parse_advanced_cql(cql_expr)

            # Generate CTE based on query node type
            if hasattr(query_node, '__class__'):
                class_name = query_node.__class__.__name__
                if class_name == 'QueryExpressionNode':
                    return self._generate_query_expression_cte(query_node, define_name)
                elif class_name == 'MultiSourceQueryNode':
                    return self._generate_multi_source_cte(query_node, define_name)
                elif class_name == 'QueryWithRelationshipsNode':
                    return self._generate_relationship_query_cte(query_node, define_name)
                elif class_name == 'CollectionOperationNode':
                    return self._generate_collection_operation_cte(query_node, define_name)
                elif class_name == 'SetOperationNode':
                    return self._generate_set_operation_cte(query_node, define_name)
                elif class_name == 'CollectionQueryNode':
                    return self._generate_collection_query_cte(query_node, define_name)
                else:
                    logger.warning(f"Unhandled query node type: {class_name}")
                    return self._create_fallback_fragment(define_name, cql_expr)
            else:
                logger.warning(f"Query expression parser returned unexpected type: {type(query_node)}")
                return self._create_fallback_fragment(define_name, cql_expr)

        except Exception as e:
            logger.error(f"Failed to parse query expression '{define_name}': {e}")
            return self._create_fallback_fragment(define_name, cql_expr)

    def _generate_query_expression_cte(self, query_node, define_name: str) -> CTEFragment:
        """
        Generate CTE SQL for QueryExpressionNode.

        Handles patterns like:
        - (4) l
        - (4) l return 'Hello World'
        - ({1, 2, 3}) l sort desc
        - ({1, 2, 3, 3, 4}) L aggregate A starting 1: A * L
        """
        cte_name = self._normalize_define_name_to_cte(define_name)

        # Extract source expression
        source_expr = query_node.source_expression

        # Determine if this is a collection or literal value
        if source_expr.startswith('{') and source_expr.endswith('}'):
            # Collection expression like {1, 2, 3}
            return self._generate_collection_cte(query_node, define_name)
        else:
            # Simple literal expression like "4"
            return self._generate_literal_value_cte(query_node, define_name)

    def _generate_collection_cte(self, query_node, define_name: str) -> CTEFragment:
        """Generate CTE for collection-based query expressions."""
        cte_name = self._normalize_define_name_to_cte(define_name)

        # Parse collection values from {1, 2, 3} format
        collection_str = query_node.source_expression.strip('{}')
        values = [v.strip().strip("'\"") for v in collection_str.split(',')]

        # Build VALUES clause
        if self.dialect == 'DUCKDB':
            values_sql = ', '.join([f"({v})" for v in values])
            select_fields = [f"{query_node.alias}.value"]
            from_clause = f"(VALUES {values_sql}) AS {query_node.alias}(value)"
        else:  # PostgreSQL
            values_sql = ', '.join([f"({v})" for v in values])
            select_fields = [f"{query_node.alias}.value"]
            from_clause = f"(VALUES {values_sql}) AS {query_node.alias}(value)"

        where_conditions = []

        # Handle sort clause
        if query_node.sort_clause:
            order_by = self._generate_order_by_clause(query_node.sort_clause, query_node.alias)
            # For CTEs, we can't directly use ORDER BY, so we'll include it as a comment
            where_conditions.append(f"/* ORDER BY {order_by} */")

        # Handle return expression
        if query_node.return_expression:
            # Modify select fields based on return expression
            return_value = query_node.return_expression.strip("'\"")
            select_fields = [f"'{return_value}' as result"]

        # Handle aggregate clause
        if query_node.aggregate_clause:
            return self._generate_aggregate_cte(query_node, define_name)

        return CTEFragment(
            name=cte_name,
            resource_type="Collection",  # Special type for collection queries
            patient_id_extraction="NULL as patient_id",  # Collections don't have patient IDs
            select_fields=select_fields,
            from_clause=from_clause,
            where_conditions=where_conditions,
            source_cql_expression=str(query_node),
            define_name=define_name,
            result_type="collection"
        )

    def _generate_literal_value_cte(self, query_node, define_name: str) -> CTEFragment:
        """Generate CTE for literal value query expressions like '(4) l'."""
        cte_name = self._normalize_define_name_to_cte(define_name)

        literal_value = query_node.source_expression

        # Handle return expression
        if query_node.return_expression:
            return_value = query_node.return_expression.strip("'\"")
            select_fields = [f"'{return_value}' as result"]
        else:
            select_fields = [f"{literal_value} as {query_node.alias}"]

        # Use dual-like table for literal values
        if self.dialect == 'DUCKDB':
            from_clause = f"(SELECT {literal_value} as value) as {query_node.alias}"
        else:  # PostgreSQL
            from_clause = f"(SELECT {literal_value} as value) as {query_node.alias}"

        return CTEFragment(
            name=cte_name,
            resource_type="Literal",  # Special type for literal queries
            patient_id_extraction="NULL as patient_id",  # Literals don't have patient IDs
            select_fields=select_fields,
            from_clause=from_clause,
            where_conditions=[],
            source_cql_expression=str(query_node),
            define_name=define_name,
            result_type="literal"
        )

    def _generate_multi_source_cte(self, query_node, define_name: str) -> CTEFragment:
        """
        Generate CTE for multi-source query expressions.

        Handles patterns like: from ({2, 3}) A, ({5, 6}) B
        """
        cte_name = self._normalize_define_name_to_cte(define_name)

        # Build CROSS JOIN for Cartesian product
        from_parts = []
        for i, (source_expr, alias) in enumerate(query_node.sources):
            # Parse collection values
            if source_expr.startswith('{') and source_expr.endswith('}'):
                collection_str = source_expr.strip('{}')
                values = [v.strip().strip("'\"") for v in collection_str.split(',')]
                values_sql = ', '.join([f"({v})" for v in values])

                if self.dialect == 'DUCKDB':
                    table_expr = f"(VALUES {values_sql}) AS {alias}(value)"
                else:  # PostgreSQL
                    table_expr = f"(VALUES {values_sql}) AS {alias}(value)"

                from_parts.append(table_expr)

        # Join all sources with CROSS JOIN
        from_clause = from_parts[0]
        for part in from_parts[1:]:
            from_clause += f" CROSS JOIN {part}"

        # Select all source values
        select_fields = []
        for source_expr, alias in query_node.sources:
            select_fields.append(f"{alias}.value as {alias}")

        # Handle aggregate clause for multi-source queries
        if query_node.aggregate_clause:
            # For multi-source aggregates, we need to handle the aggregate expression
            agg_expr = query_node.aggregate_clause.aggregate_expression
            if agg_expr:
                # Simple aggregation - this would need more sophisticated parsing
                select_fields = [f"SUM({agg_expr.replace(' ', '').replace('A', '1').replace('+', ' + ')}) as result"]

        return CTEFragment(
            name=cte_name,
            resource_type="MultiSource",  # Special type for multi-source queries
            patient_id_extraction="NULL as patient_id",  # Multi-source queries don't have patient IDs
            select_fields=select_fields,
            from_clause=from_clause,
            where_conditions=[],
            source_cql_expression=str(query_node),
            define_name=define_name,
            result_type="multi_source"
        )

    def _generate_aggregate_cte(self, query_node, define_name: str) -> CTEFragment:
        """Generate CTE for aggregate query expressions."""
        cte_name = self._normalize_define_name_to_cte(define_name)

        # Parse collection values
        source_expr = query_node.source_expression
        collection_str = source_expr.strip('{}')
        values = [v.strip().strip("'\"") for v in collection_str.split(',')]

        # Build VALUES clause
        if self.dialect == 'DUCKDB':
            values_sql = ', '.join([f"({v})" for v in values])
            from_clause = f"(VALUES {values_sql}) AS {query_node.alias}(value)"
        else:  # PostgreSQL
            values_sql = ', '.join([f"({v})" for v in values])
            from_clause = f"(VALUES {values_sql}) AS {query_node.alias}(value)"

        # Handle aggregate clause
        agg_clause = query_node.aggregate_clause
        starting_value = agg_clause.starting_expression or "0"

        # For now, handle simple multiplication aggregate: A * L
        if "* L" in agg_clause.aggregate_expression:
            if agg_clause.is_distinct:
                # Use DISTINCT values for aggregation
                select_fields = [f"COALESCE({starting_value} * (SELECT PRODUCT(DISTINCT value) FROM (VALUES {values_sql}) AS vals(value)), 0) as result"]
            else:
                # Use all values for aggregation
                agg_expr = " * ".join(values)
                if starting_value != "0":
                    agg_expr = f"{starting_value} * {agg_expr}"
                select_fields = [f"{agg_expr} as result"]
        else:
            # Fallback for other aggregate expressions
            select_fields = [f"/* Aggregate: {agg_clause.aggregate_expression} */ 0 as result"]

        return CTEFragment(
            name=cte_name,
            resource_type="Aggregate",  # Special type for aggregate queries
            patient_id_extraction="NULL as patient_id",  # Aggregates don't have patient IDs
            select_fields=select_fields,
            from_clause="(SELECT 1) as dummy",  # Simplified for aggregate results
            where_conditions=[],
            source_cql_expression=str(query_node),
            define_name=define_name,
            result_type="aggregate"
        )

    # =================================================================================
    # Relationship Expression SQL Generation (Step 2.2: Relationship Expression SQL Generation)
    # =================================================================================

    def _generate_relationship_query_cte(self, query_node, define_name: str) -> CTEFragment:
        """
        Generate CTE for relationship query expressions with with/without clauses.

        This method converts QueryWithRelationshipsNode objects into SQL CTEs that
        use JOINs to implement the relationship logic.

        Args:
            query_node: QueryWithRelationshipsNode representing the relationship query
            define_name: Name of the CQL define

        Returns:
            CTEFragment representing the relationship query as a CTE
        """
        logger.debug(f"Generating relationship query CTE for: {define_name}")

        try:
            cte_name = self._normalize_define_name_to_cte(define_name)

            # Extract primary resource information
            primary_resource_type = self._extract_resource_type_from_query(query_node.primary_query)
            primary_alias = query_node.alias

            # Build the base FROM clause
            base_table = self._get_table_name_for_resource(primary_resource_type)
            from_clause = f"{base_table} {primary_alias}"

            # Build JOINs for with/without clauses
            join_clauses = []
            where_conditions = []
            select_fields = [f"{primary_alias}.*"]

            for with_clause in query_node.with_clauses:
                join_sql, additional_conditions, additional_fields = self._generate_with_clause_join(
                    with_clause, primary_alias, primary_resource_type
                )
                join_clauses.append(join_sql)
                where_conditions.extend(additional_conditions)
                select_fields.extend(additional_fields)

            # Combine FROM and JOIN clauses
            full_from_clause = from_clause + " " + " ".join(join_clauses)

            # Add WHERE condition if present
            if query_node.where_condition:
                where_conditions.append(f"({query_node.where_condition})")

            # Handle RETURN clause
            if query_node.return_expression:
                select_fields = self._parse_return_expression(query_node.return_expression, primary_alias)

            # Extract patient ID (assuming primary resource has patient relationship)
            patient_id_extraction = self._generate_patient_id_extraction(primary_resource_type, primary_alias)

            return CTEFragment(
                name=cte_name,
                resource_type=primary_resource_type,
                patient_id_extraction=patient_id_extraction,
                select_fields=select_fields,
                from_clause=full_from_clause,
                where_conditions=where_conditions,
                source_cql_expression=str(query_node),
                define_name=define_name,
                result_type="relationship_query"
            )

        except Exception as e:
            logger.error(f"Failed to generate relationship query CTE for '{define_name}': {e}")
            # Fallback to simple resource query
            resource_type = self._extract_resource_type_from_query(query_node.primary_query)
            return self._create_resource_fragment(define_name, resource_type)

    def _generate_with_clause_join(self, with_clause, primary_alias: str, primary_resource_type: str) -> Tuple[str, List[str], List[str]]:
        """
        Generate SQL JOIN for a with/without clause.

        Args:
            with_clause: WithClauseNode representing the with/without clause
            primary_alias: Alias of the primary resource
            primary_resource_type: Type of the primary resource

        Returns:
            Tuple of (join_sql, additional_conditions, additional_select_fields)
        """
        # Extract related resource information
        related_resource_type = self._extract_resource_type_from_query(with_clause.related_query)
        related_alias = with_clause.source_alias
        related_table = self._get_table_name_for_resource(related_resource_type)

        # Determine JOIN type
        if with_clause.is_without:
            join_type = "LEFT JOIN"
        else:
            join_type = "INNER JOIN"

        # Generate JOIN condition from such that clause
        join_condition = self._generate_such_that_sql(with_clause.condition, primary_alias, related_alias)

        # Build the JOIN SQL
        join_sql = f"{join_type} {related_table} {related_alias} ON {join_condition}"

        # Additional conditions and fields
        additional_conditions = []
        additional_fields = []

        # For WITHOUT clauses, add NULL check
        if with_clause.is_without:
            additional_conditions.append(f"{related_alias}.id IS NULL")

        # Add related resource fields to SELECT
        additional_fields.append(f"{related_alias}.id as {related_alias}_id")

        return join_sql, additional_conditions, additional_fields

    def _generate_such_that_sql(self, such_that_condition, primary_alias: str, related_alias: str) -> str:
        """
        Generate SQL for such that conditions with proper reference and temporal handling.

        Args:
            such_that_condition: SuchThatConditionNode representing the condition
            primary_alias: Alias of the primary resource
            related_alias: Alias of the related resource

        Returns:
            SQL condition string
        """
        if hasattr(such_that_condition, 'condition_text'):
            condition_text = such_that_condition.condition_text
        else:
            condition_text = str(such_that_condition)

        # Handle reference patterns
        if hasattr(such_that_condition, 'has_references') and such_that_condition.has_references():
            return self._generate_reference_condition_sql(such_that_condition, primary_alias, related_alias)

        # Handle temporal conditions
        if hasattr(such_that_condition, 'has_temporal_conditions') and such_that_condition.has_temporal_conditions():
            return self._generate_temporal_condition_sql(such_that_condition, primary_alias, related_alias)

        # Fallback: simple condition replacement
        condition_sql = condition_text
        condition_sql = condition_sql.replace(f"{related_alias}.", f"{related_alias}.")
        condition_sql = condition_sql.replace(f"{primary_alias}.", f"{primary_alias}.")

        return condition_sql

    def _generate_reference_condition_sql(self, such_that_condition, primary_alias: str, related_alias: str) -> str:
        """Generate SQL for FHIR reference conditions."""
        conditions = []

        for ref_pattern in such_that_condition.reference_patterns:
            source_alias = ref_pattern['source_alias']
            source_field = ref_pattern['source_field']
            target_alias = ref_pattern['target_alias']
            target_field = ref_pattern['target_field']

            if source_field == 'subject':
                # Handle FHIR subject reference: O.subject references P.id or O.subject references P.subject
                # In FHIR, subject references resolve to the resource ID
                actual_target_field = 'id' if target_field == 'subject' else target_field
                resource_type = self._get_resource_type_for_reference(target_alias)
                subject_reference = self._extract_json_text(f"{source_alias}.resource", "$.subject.reference")
                condition = f"{subject_reference} = CONCAT('{resource_type}/', {target_alias}.{actual_target_field})"
            else:
                # Generic reference handling
                reference_path = f"$.{source_field}.reference"
                reference_extract = self._extract_json_text(f"{source_alias}.resource", reference_path)
                resource_type = self._get_resource_type_for_reference(target_alias)
                condition = f"{reference_extract} = CONCAT('{resource_type}/', {target_alias}.{target_field})"

            conditions.append(condition)

        # Handle logical operators
        if hasattr(such_that_condition, 'logical_operators') and such_that_condition.logical_operators:
            # Simple handling for now - join with AND
            return " AND ".join(conditions)
        else:
            return conditions[0] if conditions else "1=1"

    def _generate_temporal_condition_sql(self, such_that_condition, primary_alias: str, related_alias: str) -> str:
        """Generate SQL for temporal conditions."""
        conditions = []

        for temporal in such_that_condition.temporal_conditions:
            alias = temporal['alias']
            field = temporal['field']
            operator = temporal['operator'].lower()
            operand = temporal['operand']

            # Extract the field value
            field_sql = self._extract_json_text(f"{alias}.resource", f"$.{field}")

            # Handle different temporal operators
            if operator == 'during':
                # Field value should be within the operand period
                if operand.startswith('"') and operand.endswith('"'):
                    # Parameter reference like "Measurement Period"
                    operand_clean = operand.strip('"')
                    # For now, use a simplified BETWEEN approach
                    # In production, this would integrate with parameter resolution
                    condition = f"{field_sql}::date BETWEEN '{operand_clean}_start'::date AND '{operand_clean}_end'::date"
                else:
                    # Direct period reference or field reference
                    if "." in operand:
                        # Field reference like E.period
                        condition = f"{field_sql} DURING {operand}"
                    else:
                        # Simple operand
                        condition = f"{field_sql} DURING '{operand}'"
            elif operator == 'overlaps':
                condition = f"{field_sql} OVERLAPS {operand}"
            elif operator in ['before', 'after']:
                condition = f"{field_sql}::date {operator.upper()} {operand}::date"
            else:
                # Fallback for other temporal operators
                condition = f"{field_sql} {operator.upper()} {operand}"

            conditions.append(condition)

        return " AND ".join(conditions) if conditions else "1=1"

    def _extract_resource_type_from_query(self, resource_query: str) -> str:
        """Extract resource type from query like '[Patient]' or '[Observation: \"HbA1c\"]'."""
        import re
        match = re.search(r'\[([^\]:]+)', resource_query)
        if match:
            return match.group(1).strip()
        return "Unknown"

    def _get_table_name_for_resource(self, resource_type: str) -> str:
        """Get database table name for a FHIR resource type."""
        # Use existing pattern from the codebase
        return resource_type.lower()

    def _get_resource_type_for_reference(self, alias: str) -> str:
        """Determine resource type for a reference based on alias context."""
        # Simple heuristic - could be enhanced based on context
        common_mappings = {
            'P': 'Patient',
            'Patient': 'Patient',
            'O': 'Observation',
            'Obs': 'Observation',
            'E': 'Encounter',
            'C': 'Condition'
        }
        return common_mappings.get(alias, 'Resource')

    def _generate_patient_id_extraction(self, resource_type: str, alias: str) -> str:
        """Generate patient ID extraction SQL for a resource."""
        if resource_type.lower() == 'patient':
            return f"{alias}.id as patient_id"
        else:
            # Extract patient reference from subject field
            subject_reference = self._extract_json_text(f"{alias}.resource", "$.subject.reference")
            if self.dialect == 'DUCKDB':
                return f"REGEXP_EXTRACT({subject_reference}, 'Patient/(.+)') as patient_id"
            else:  # PostgreSQL
                return f"REGEXP_REPLACE({subject_reference}, 'Patient/', '') as patient_id"

    def _parse_return_expression(self, return_expression: str, primary_alias: str) -> List[str]:
        """Parse CQL return expression into SQL SELECT fields."""
        # Simple parsing for common patterns
        if return_expression.startswith('{') and return_expression.endswith('}'):
            # Tuple return: {patient: P, condition: C}
            import re
            field_matches = re.findall(r'(\w+):\s*(\w+)', return_expression)
            fields = []
            for field_name, alias in field_matches:
                fields.append(f"{alias}.* as {field_name}")
            return fields if fields else [f"{primary_alias}.*"]
        else:
            # Simple alias return: return P
            if return_expression.strip() in [primary_alias]:
                return [f"{primary_alias}.*"]
            else:
                return [f"{return_expression} as result"]

    def _extract_json_text(self, column: str, path: str) -> str:
        """Extract JSON text field with dialect-specific handling."""
        if self.dialect == 'DUCKDB':
            return f"json_extract_string({column}, '{path}')"
        else:  # PostgreSQL
            # Convert JSONPath to PostgreSQL format if needed
            if path.startswith('$.'):
                path_parts = path[2:].split('.')
                return f"jsonb_extract_path_text({column}, {', '.join(repr(part) for part in path_parts)})"
            else:
                return f"jsonb_extract_path_text({column}, '{path}')"

    def _generate_order_by_clause(self, sort_clause, alias: str) -> str:
        """Generate ORDER BY clause from sort clause."""
        direction = sort_clause.direction.upper()
        if direction in ['ASC', 'ASCENDING']:
            direction = 'ASC'
        elif direction in ['DESC', 'DESCENDING']:
            direction = 'DESC'

        if sort_clause.sort_expression:
            return f"{sort_clause.sort_expression} {direction}"
        else:
            return f"{alias}.value {direction}"

    def _generate_collection_operation_cte(self, query_node, define_name: str) -> CTEFragment:
        """
        Generate CTE for collection operations (flatten, distinct, union, intersect, etc.).

        Args:
            query_node: CollectionOperationNode representing the collection operation
            define_name: Name of the CQL define

        Returns:
            CTEFragment representing the collection operation as a CTE
        """
        logger.debug(f"Generating collection operation CTE for: {define_name}")

        try:
            cte_name = self._normalize_define_name_to_cte(define_name)
            operation_type = query_node.operation_type

            # For now, generate a simplified CTE for collection operations
            # This is a basic implementation that can be enhanced later
            base_expr = str(query_node.base_expression)

            # Generate basic SQL based on operation type
            if operation_type == "flatten":
                # Basic flatten implementation
                operation_sql = f"json_array_flatten({base_expr})"
            elif operation_type == "distinct":
                # Basic distinct implementation
                operation_sql = f"json_array_distinct({base_expr})"
            elif operation_type in ["distinctby", "unionby", "intersectby"]:
                # Basic predicate operation implementation
                predicate_expr = str(query_node.predicate_expression) if query_node.predicate_expression else ""
                operation_sql = f"{operation_type}({base_expr}, {predicate_expr})"
            else:
                # Generic operation
                operation_sql = f"{operation_type}({base_expr})"

            # Build the CTE with a simplified structure
            from_clause = f"(SELECT {operation_sql} as result) as collection_result"
            select_fields = ["result"]
            where_conditions = []

            # For collection operations, result type is typically a collection
            result_type = "collection"
            resource_type = "Collection"

            return CTEFragment(
                name=cte_name,
                resource_type=resource_type,
                patient_id_extraction="NULL as patient_id",  # Collection operations may not have patient context
                select_fields=select_fields,
                from_clause=from_clause,
                where_conditions=where_conditions,
                source_cql_expression=str(query_node),
                define_name=define_name,
                result_type=result_type
            )

        except Exception as e:
            logger.error(f"Failed to generate collection operation CTE for '{define_name}': {e}")
            return self._create_fallback_fragment(define_name, str(query_node))

    def _generate_set_operation_cte(self, query_node, define_name: str) -> CTEFragment:
        """
        Generate CTE for set operations (union, intersect, except).

        Args:
            query_node: SetOperationNode representing the set operation
            define_name: Name of the CQL define

        Returns:
            CTEFragment representing the set operation as a CTE
        """
        logger.debug(f"Generating set operation CTE for: {define_name}")

        try:
            cte_name = self._normalize_define_name_to_cte(define_name)
            operator = query_node.operator

            # Generate SQL for left and right expressions
            left_expr = str(query_node.left_expression)
            right_expr = str(query_node.right_expression)

            # Handle different set operations
            if operator == 'union':
                set_sql = f"SELECT * FROM ({left_expr}) UNION SELECT * FROM ({right_expr})"
            elif operator == 'intersect':
                set_sql = f"SELECT * FROM ({left_expr}) INTERSECT SELECT * FROM ({right_expr})"
            elif operator == 'except':
                set_sql = f"SELECT * FROM ({left_expr}) EXCEPT SELECT * FROM ({right_expr})"
            else:
                raise ValueError(f"Unsupported set operation: {operator}")

            # If custom equality predicate is provided, use more complex logic
            if query_node.has_custom_equality():
                # Use collection function handler for predicate-based operations
                from ...cql.functions.collection_functions import CQLCollectionFunctionHandler
                collection_handler = CQLCollectionFunctionHandler(dialect=self.dialect)

                operation_name = f"{operator}by"
                mock_func_node = type('MockFunctionNode', (), {
                    'args': [right_expr, query_node.equality_predicate]
                })()

                set_sql = collection_handler.handle_function(operation_name, left_expr, mock_func_node)

            from_clause = f"({set_sql}) as set_operation_result"
            select_fields = ["*"]
            where_conditions = []

            # Determine resource type from left expression
            resource_type = "Collection"  # Default for set operations

            return CTEFragment(
                name=cte_name,
                resource_type=resource_type,
                patient_id_extraction="NULL as patient_id",
                select_fields=select_fields,
                from_clause=from_clause,
                where_conditions=where_conditions,
                source_cql_expression=str(query_node),
                define_name=define_name,
                result_type="set_operation"
            )

        except Exception as e:
            logger.error(f"Failed to generate set operation CTE for '{define_name}': {e}")
            return self._create_fallback_fragment(define_name, str(query_node))

    def _generate_collection_query_cte(self, query_node, define_name: str) -> CTEFragment:
        """
        Generate CTE for complex collection queries with multiple clauses.

        Args:
            query_node: CollectionQueryNode representing the complex collection query
            define_name: Name of the CQL define

        Returns:
            CTEFragment representing the collection query as a CTE
        """
        logger.debug(f"Generating collection query CTE for: {define_name}")

        try:
            cte_name = self._normalize_define_name_to_cte(define_name)

            # Generate base FROM clause from source expression
            base_source = str(query_node.source_expression)
            alias = query_node.alias or "col"

            # Build the query components
            select_fields = [f"{alias}.*"]
            from_clause = f"({base_source}) as {alias}"
            where_conditions = []

            # Add WHERE conditions
            for condition in query_node.where_conditions:
                where_conditions.append(f"({condition})")

            # Handle LET expressions (as CTEs)
            let_ctes = []
            for let_expr in query_node.let_expressions:
                let_cte_name = f"{cte_name}_{let_expr.variable_name}"
                let_ctes.append(f"{let_cte_name} AS (SELECT {let_expr.expression} as {let_expr.variable_name})")

            # Handle RETURN expression
            if query_node.return_expression:
                return_expr = str(query_node.return_expression)
                select_fields = [return_expr]

            # Handle SORT clauses
            order_by_clauses = []
            for sort_clause in query_node.sort_clauses:
                order_by_sql = self._generate_order_by_clause(sort_clause, alias)
                order_by_clauses.append(order_by_sql)

            # Handle collection operations
            for collection_op in query_node.collection_operations:
                # Apply collection operations to the current query
                op_cte = self._generate_collection_operation_cte(collection_op, f"{define_name}_op")
                # This would require more complex chaining logic
                pass

            # Combine all parts
            if let_ctes:
                from_clause = f"WITH {', '.join(let_ctes)} SELECT * FROM {from_clause}"

            if order_by_clauses:
                # Add ORDER BY to the CTE (though this may have limitations)
                from_clause += f" ORDER BY {', '.join(order_by_clauses)}"

            # Handle aggregation
            if query_node.aggregation_clause:
                # Generate aggregation SQL
                agg_clause = query_node.aggregation_clause
                agg_sql = f"{agg_clause.aggregate_expression}"
                select_fields = [f"{agg_sql} as {agg_clause.result_alias}"]

            resource_type = "Collection"  # Default for collection queries

            return CTEFragment(
                name=cte_name,
                resource_type=resource_type,
                patient_id_extraction="NULL as patient_id",
                select_fields=select_fields,
                from_clause=from_clause,
                where_conditions=where_conditions,
                source_cql_expression=str(query_node),
                define_name=define_name,
                result_type="collection_query"
            )

        except Exception as e:
            logger.error(f"Failed to generate collection query CTE for '{define_name}': {e}")
            return self._create_fallback_fragment(define_name, str(query_node))