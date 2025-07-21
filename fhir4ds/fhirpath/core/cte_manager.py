"""
Common Table Expression (CTE) management for FHIRPath SQL generation.

This module provides centralized CTE management functionality that was previously
embedded in the monolithic SQLGenerator class.
"""

import hashlib
import re
from typing import Dict, Set, Optional, List, Tuple


class CTEManager:
    """
    Manages Common Table Expressions (CTEs) for SQL generation.
    
    This class handles CTE creation, optimization, deduplication, and inlining
    to improve SQL query performance and maintainability.
    """
    
    def __init__(self, table_name: str = "fhir_resources"):
        """
        Initialize CTE manager.
        
        Args:
            table_name: Name of the main FHIR resources table
        """
        self.table_name = table_name
        self.ctes: Dict[str, str] = {}  # CTE name -> CTE SQL
        self.cte_aliases: Dict[str, str] = {}  # Expression hash -> CTE name
        self.cte_counter = 0
        self.inlined_ctes: Dict[str, str] = {}  # Track CTEs that have been inlined
        
        # CTE configuration
        self.enable_cte = True
        self.cte_configs = self._setup_cte_configs()
        
        # Performance thresholds
        self.cte_usage_threshold = 2
        self.simple_cte_inline_threshold = 50  # characters
        self.expensive_operations = ['WHERE', 'GROUP BY', 'ORDER BY', 'HAVING']
    
    def _setup_cte_configs(self) -> Dict[str, Dict]:
        """Setup CTE configuration system."""
        return {
            'where': {'enabled': True, 'priority': 'high'},
            'select': {'enabled': True, 'priority': 'high'},
            'first': {'enabled': True, 'priority': 'medium'},
            'last': {'enabled': True, 'priority': 'medium'},
            'count': {'enabled': True, 'priority': 'medium'},
            'exists': {'enabled': True, 'priority': 'medium'},
            'empty': {'enabled': True, 'priority': 'medium'},
            'substring': {'enabled': True, 'priority': 'low'},
            'contains': {'enabled': True, 'priority': 'low'},
            'startswith': {'enabled': True, 'priority': 'low'},
            'endswith': {'enabled': True, 'priority': 'low'},
            'array_extraction': {'enabled': True, 'priority': 'high'},
            'toboolean': {'enabled': True, 'priority': 'medium'},
            'todecimal': {'enabled': True, 'priority': 'medium'},
            'todate': {'enabled': True, 'priority': 'medium'},
            'todatetime': {'enabled': True, 'priority': 'medium'},
            'totime': {'enabled': True, 'priority': 'medium'},
            'tointeger': {'enabled': True, 'priority': 'medium'},
            'tostring': {'enabled': True, 'priority': 'medium'},
            'convertstoboolean': {'enabled': True, 'priority': 'medium'},
            'convertstodecimal': {'enabled': True, 'priority': 'medium'},
            'convertstointeger': {'enabled': True, 'priority': 'medium'},
            'convertstodate': {'enabled': True, 'priority': 'medium'},
            'convertstodatetime': {'enabled': True, 'priority': 'medium'},
            'convertstotime': {'enabled': True, 'priority': 'medium'},
            'getValue': {'enabled': True, 'priority': 'high'},
        }
    
    def should_use_cte_unified(self, base_expr: str, function_name: str) -> bool:
        """
        Unified CTE decision system.
        
        Args:
            base_expr: Base SQL expression
            function_name: Name of the function being processed
            
        Returns:
            Boolean indicating whether to use CTE for this function
        """
        if not self.enable_cte:
            return False
            
        # Check if function is configured for CTE usage
        config = self.cte_configs.get(function_name, {})
        if not config.get('enabled', False):
            return False
            
        # Simple expressions don't need CTEs
        if self._is_simple_expression(base_expr):
            return False
            
        # Complex expressions benefit from CTEs
        if self._is_complex_expression(base_expr):
            return True
            
        # Default based on function priority
        priority = config.get('priority', 'low')
        return priority in ['high', 'medium']
    
    def _is_simple_expression(self, expr: str) -> bool:
        """Check if expression is simple enough to not need CTE."""
        if not expr or len(expr) < 20:
            return True
            
        # Single column references are simple
        if expr.count(' ') <= 2 and not any(op in expr for op in ['SELECT', 'FROM', 'WHERE', 'CASE']):
            return True
            
        return False
    
    def _is_complex_expression(self, expr: str) -> bool:
        """Check if expression is complex enough to benefit from CTE."""
        if not expr:
            return False
            
        # Count complexity indicators
        complexity_indicators = [
            'SELECT', 'FROM', 'WHERE', 'CASE', 'WHEN', 'THEN',
            'JOIN', 'GROUP BY', 'ORDER BY', 'HAVING', 'UNION'
        ]
        
        complexity_score = sum(1 for indicator in complexity_indicators if indicator in expr.upper())
        return complexity_score >= 3
    
    def create_cte(self, sql_query: str, base_name: str) -> str:
        """
        Create a CTE with deduplication support.
        
        Args:
            sql_query: The SQL query for the CTE
            base_name: Base name for the CTE
            
        Returns:
            The CTE alias name
        """
        # Create fingerprint for deduplication
        fingerprint = self._create_expression_fingerprint(sql_query)
        
        # Check if we already have a CTE for this expression
        if fingerprint in self.cte_aliases:
            return self.cte_aliases[fingerprint]
        
        # Create unique CTE name
        cte_name = f"{base_name}_{self.cte_counter}"
        self.cte_counter += 1
        
        # Store CTE
        self.ctes[cte_name] = sql_query
        self.cte_aliases[fingerprint] = cte_name
        
        return cte_name
    
    def _create_expression_fingerprint(self, expression: str) -> str:
        """Create a fingerprint for expression deduplication."""
        # Normalize the expression for better deduplication
        normalized = re.sub(r'\s+', ' ', expression.strip())
        normalized = normalized.lower()
        
        # Create hash
        return hashlib.md5(normalized.encode()).hexdigest()[:8]
    
    def build_final_query_with_ctes(self, main_query: str) -> str:
        """
        Build the final query with CTEs.
        
        Args:
            main_query: The main SQL query
            
        Returns:
            Complete SQL query with CTEs
        """
        if not self.ctes:
            return main_query
        
        # Apply smart inlining for simple CTEs
        optimized_ctes = self._apply_cte_inlining()
        
        if not optimized_ctes:
            return main_query
        
        # Build CTE clause
        cte_clauses = []
        for cte_name, cte_sql in optimized_ctes.items():
            cte_clauses.append(f"{cte_name} AS (\n{cte_sql}\n)")
        
        # Combine with main query
        return f"WITH {', '.join(cte_clauses)}\n{main_query}"
    
    def _apply_cte_inlining(self) -> Dict[str, str]:
        """Apply smart inlining for simple CTEs."""
        optimized_ctes = {}
        
        for cte_name, cte_sql in self.ctes.items():
            # Check if CTE should be inlined
            if self._should_inline_cte(cte_name, cte_sql):
                # Store inlined CTE for reference replacement
                self.inlined_ctes[cte_name] = cte_sql
            else:
                # Keep as regular CTE
                optimized_ctes[cte_name] = cte_sql
        
        return optimized_ctes
    
    def _should_inline_cte(self, cte_name: str, cte_sql: str) -> bool:
        """Determine if a CTE should be inlined."""
        # Phase 4.6: Don't inline exists_check CTEs to prevent SQL syntax errors
        if cte_name.startswith('exists_check'):
            return False
            
        # Don't inline CTEs that use specific result column names
        if 'exists_result' in cte_sql:
            return False
            
        # Simple CTEs can be inlined
        if self._is_simple_cte(cte_sql):
            return True
            
        # CTEs used only once can be inlined
        if self._count_cte_usage(cte_name) <= 1:
            return True
            
        # Don't inline expensive operations
        if self._contains_expensive_operations(cte_sql):
            return False
            
        return False
    
    def _is_simple_cte(self, cte_sql: str) -> bool:
        """Check if CTE is simple enough to inline."""
        return len(cte_sql) <= self.simple_cte_inline_threshold
    
    def _count_cte_usage(self, cte_name: str) -> int:
        """Count how many times a CTE is used."""
        # For now, assume CTEs are used multiple times to prevent aggressive inlining
        # This encourages proper CTE usage for complex expressions
        return 2
    
    def _contains_expensive_operations(self, sql: str) -> bool:
        """Check if SQL contains expensive operations."""
        sql_upper = sql.upper()
        return any(op in sql_upper for op in self.expensive_operations)
    
    def get_cte_expression(self, cte_name: str) -> Optional[str]:
        """Get the SQL expression for a CTE."""
        return self.ctes.get(cte_name)
    
    def clear_ctes(self):
        """Clear all CTEs and reset state."""
        self.ctes.clear()
        self.cte_aliases.clear()
        self.inlined_ctes.clear()
        self.cte_counter = 0
    
    def get_cte_count(self) -> int:
        """Get the number of active CTEs."""
        return len(self.ctes)
    
    def get_cte_names(self) -> List[str]:
        """Get list of all CTE names."""
        return list(self.ctes.keys())