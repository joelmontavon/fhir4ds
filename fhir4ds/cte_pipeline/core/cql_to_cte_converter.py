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
    
    # Resource type patterns from existing detection logic
    RESOURCE_TYPE_PATTERNS = {
        'Patient': {
            'indicators': ['[Patient]', 'patient', 'AgeInYears()', 'birthDate', 'gender'],
            'priority': 10
        },
        'Condition': {
            'indicators': ['[Condition', 'asthma', 'diabetes', 'hypertension', 'diagnosis', 'condition'],
            'priority': 8
        },
        'MedicationDispense': {
            'indicators': ['[MedicationDispense]', 'medication', 'dispense', 'medicationCodeableConcept'],
            'priority': 7
        },
        'Encounter': {
            'indicators': ['[Encounter]', 'encounter', 'visit', 'hospitalization', 'admission'],
            'priority': 6
        },
        'Observation': {
            'indicators': ['[Observation', 'observation', 'vital', 'lab', 'measurement'],
            'priority': 5
        },
        'Procedure': {
            'indicators': ['[Procedure]', 'procedure', 'surgery', 'operation'],
            'priority': 4
        },
        'DiagnosticReport': {
            'indicators': ['[DiagnosticReport]', 'diagnostic', 'report', 'result'],
            'priority': 3
        },
        'Immunization': {
            'indicators': ['[Immunization]', 'immunization', 'vaccine', 'vaccination'],
            'priority': 2
        }
    }
    
    def detect_from_cql(self, cql_expr: str, define_name: str = "") -> str:
        """
        Detect FHIR resource type from CQL expression.
        
        Leverages existing patterns from functions.py with enhanced analysis
        including define name context and stack analysis integration.
        
        Args:
            cql_expr: CQL expression to analyze
            define_name: Define name for additional context
            
        Returns:
            Detected FHIR resource type
        """
        cql_lower = cql_expr.lower()
        define_lower = define_name.lower()
        
        # Score each resource type based on pattern matches
        scores = {}
        
        for resource_type, config in self.RESOURCE_TYPE_PATTERNS.items():
            score = 0
            
            # Check CQL expression patterns
            for indicator in config['indicators']:
                if indicator.lower() in cql_lower:
                    # Exact bracket match gets higher score
                    if indicator.startswith('[') and indicator.endswith(']'):
                        score += 20
                    else:
                        score += config['priority']
            
            # Check define name patterns  
            for indicator in config['indicators']:
                if indicator.lower() in define_lower:
                    score += config['priority'] // 2
            
            if score > 0:
                scores[resource_type] = score
        
        if scores:
            # Return resource type with highest score
            detected_type = max(scores.items(), key=lambda x: x[1])[0]
            logger.debug(f"Detected resource type '{detected_type}' for CQL: '{cql_expr[:50]}...' (scores: {scores})")
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
    
    def __init__(self, dialect: str, terminology_client=None):
        """
        Initialize CQL to CTE converter.
        
        Args:
            dialect: Database dialect ('duckdb' or 'postgresql')
            terminology_client: Optional terminology client for value set resolution
        """
        self.dialect = dialect.upper()
        self.terminology_client = terminology_client
        self.resource_type_detector = ResourceTypeDetector()
        self.pattern_analyzer = CQLPatternAnalyzer(dialect)
        
        # Task 3.2 Enhancement: Enhanced dependency detection
        self.dependency_detector = EnhancedDependencyDetector()
        
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
        
        # Combine all conditions
        all_conditions = where_conditions + terminology_conditions
        
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
                # Expand value set using existing terminology client
                expanded_codes = self.terminology_client.expand_value_set(value_set_ref)
                
                if expanded_codes:
                    # Generate IN clause with expanded codes
                    code_list = "', '".join(expanded_codes)
                    
                    if self.dialect == 'DUCKDB':
                        conditions.extend([
                            f"json_extract_string(resource, '$.code.coding[0].code') IN ('{code_list}')",
                            f"EXISTS (SELECT 1 FROM json_each(json_extract(resource, '$.code.coding')) AS coding WHERE json_extract_string(coding.value, '$.code') IN ('{code_list}'))"
                        ])
                    else:  # PostgreSQL
                        conditions.extend([
                            f"jsonb_extract_path_text(resource, 'code', 'coding', '0', 'code') IN ('{code_list}')",
                            f"EXISTS (SELECT 1 FROM jsonb_array_elements(jsonb_extract_path(resource, 'code', 'coding')) AS coding WHERE coding ->> 'code' IN ('{code_list}'))"
                        ])
                
                else:
                    logger.warning(f"No codes found for value set: {value_set_ref}")
        
        except Exception as e:
            logger.warning(f"Terminology resolution failed: {e}")
            conditions.extend(self._generate_text_matching_conditions(cql_expr, resource_type))
        
        return conditions
    
    def _extract_value_set_references(self, cql_expr: str) -> List[str]:
        """Extract value set references from CQL expression."""
        value_set_refs = []
        
        # Pattern for value set in brackets: [Condition: "ValueSetName"]
        bracket_pattern = r'\[[^\]]+:\s*["\']([^"\']+)["\']\]'
        matches = re.finditer(bracket_pattern, cql_expr, re.IGNORECASE)
        
        for match in matches:
            value_set_refs.append(match.group(1))
        
        # Pattern for explicit value set references: in "ValueSetOID"
        in_pattern = r'in\s+["\']([^"\']+)["\']'
        matches = re.finditer(in_pattern, cql_expr, re.IGNORECASE)
        
        for match in matches:
            value_set_refs.append(match.group(1))
        
        return value_set_refs
    
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