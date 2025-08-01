"""
CQL Parser - Extends FHIRPath parser with CQL-specific constructs.

This parser handles CQL syntax by building on the existing FHIRPath parser
foundation and adding CQL-specific language constructs.
"""

import logging
from typing import List, Dict, Any, Optional, Union
from enum import Enum

# Import existing FHIRPath parser infrastructure  
from ...fhirpath.parser.ast_nodes import *
from ...fhirpath.parser.parser import FHIRPathParser, FHIRPathLexer, TokenType

logger = logging.getLogger(__name__)

# Extend TokenType enum with CQL-specific tokens
class CQLTokenType(Enum):
    """CQL-specific token types extending FHIRPath tokens."""
    # CQL Keywords
    LIBRARY = "LIBRARY"
    VERSION = "VERSION" 
    USING = "USING"
    INCLUDE = "INCLUDE"
    CALLED = "CALLED"
    PARAMETER = "PARAMETER"
    CONTEXT = "CONTEXT"
    DEFINE = "DEFINE"
    PUBLIC = "PUBLIC"
    PRIVATE = "PRIVATE"
    
    # CQL Operators and Constructs
    RETRIEVE = "RETRIEVE"  # [ResourceType]
    FROM = "FROM"
    WHERE = "WHERE"
    RETURN = "RETURN"
    SORT = "SORT"
    ASC = "ASC"
    DESC = "DESC"
    
    # CQL-specific operators
    IN = "IN"
    CONTAINS = "CONTAINS"
    PROPERLY = "PROPERLY"
    DURING = "DURING"
    INCLUDES = "INCLUDES"
    INCLUDEDLN = "INCLUDEDLN"
    BEFORE = "BEFORE" 
    AFTER = "AFTER"
    MEETS = "MEETS"
    OVERLAPS = "OVERLAPS"
    STARTS = "STARTS"
    ENDS = "ENDS"
    
    # Terminology
    VALUESET = "VALUESET"
    CODE = "CODE"
    CONCEPT = "CONCEPT"
    CODESYSTEM = "CODESYSTEM"

class CQLASTNode(ASTNode):
    """Base class for CQL-specific AST nodes."""
    pass

class LibraryNode(CQLASTNode):
    """AST node for CQL library definition."""
    def __init__(self, name: str, version: Optional[str] = None):
        self.name = name
        self.version = version
        self.includes = []
        self.parameters = []
        self.definitions = []
        self.context = "Patient"  # Default context

class IncludeNode(CQLASTNode):
    """AST node for CQL include statement."""
    def __init__(self, library_name: str, version: Optional[str] = None, alias: Optional[str] = None):
        self.library_name = library_name
        self.version = version
        self.alias = alias

class ParameterNode(CQLASTNode):
    """AST node for CQL parameter definition."""
    def __init__(self, name: str, parameter_type: Optional[str] = None, default_value: Optional[Any] = None):
        self.name = name
        self.parameter_type = parameter_type
        self.default_value = default_value

class ContextNode(CQLASTNode):
    """AST node for CQL context definition."""
    def __init__(self, context_name: str):
        self.context_name = context_name

class DefineNode(CQLASTNode):
    """AST node for CQL define statement."""
    def __init__(self, name: str, expression: Any, access_level: str = "PRIVATE"):
        self.name = name
        self.expression = expression
        self.access_level = access_level

class RetrieveNode(CQLASTNode):
    """AST node for CQL retrieve expression."""
    def __init__(self, resource_type: str, terminology: Optional[str] = None):
        self.resource_type = resource_type
        self.terminology = terminology

class QueryNode(CQLASTNode):
    """AST node for CQL query expression."""
    def __init__(self, source: Any, aliases: Optional[List[str]] = None, where_clause: Optional[Any] = None, 
                 return_clause: Optional[Any] = None, sort_clause: Optional[Any] = None):
        self.source = source
        self.aliases = aliases or []
        self.where_clause = where_clause 
        self.return_clause = return_clause
        self.sort_clause = sort_clause

class WithClauseNode(CQLASTNode):
    """AST node for CQL with clause in queries."""
    def __init__(self, identifier: str, expression: Any):
        self.identifier = identifier
        self.expression = expression

class SortClauseNode(CQLASTNode):
    """AST node for CQL sort clause."""
    def __init__(self, expression: Any, direction: str = "ASC"):
        self.expression = expression
        self.direction = direction

class LetClauseNode(CQLASTNode):
    """AST node for CQL let clause.""" 
    def __init__(self, identifier: str, expression: Any):
        self.identifier = identifier
        self.expression = expression

class CQLLexer(FHIRPathLexer):
    """
    CQL Lexer extending FHIRPath lexer with CQL-specific tokens.
    """
    
    def __init__(self, text: str):
        super().__init__(text)
        # Add CQL keywords to the keywords dictionary
        self.cql_keywords = {
            'library': CQLTokenType.LIBRARY,
            'version': CQLTokenType.VERSION,
            'using': CQLTokenType.USING,
            'include': CQLTokenType.INCLUDE,
            'called': CQLTokenType.CALLED,
            'parameter': CQLTokenType.PARAMETER,
            'context': CQLTokenType.CONTEXT,
            'define': CQLTokenType.DEFINE,
            'public': CQLTokenType.PUBLIC,
            'private': CQLTokenType.PRIVATE,
            'from': CQLTokenType.FROM,
            'where': CQLTokenType.WHERE,
            'return': CQLTokenType.RETURN,
            'sort': CQLTokenType.SORT,
            'asc': CQLTokenType.ASC,
            'desc': CQLTokenType.DESC,
            'in': CQLTokenType.IN,
            'contains': CQLTokenType.CONTAINS,
            'during': CQLTokenType.DURING,
            'includes': CQLTokenType.INCLUDES,
            'before': CQLTokenType.BEFORE,
            'after': CQLTokenType.AFTER,
            'meets': CQLTokenType.MEETS,
            'overlaps': CQLTokenType.OVERLAPS,
            'starts': CQLTokenType.STARTS,
            'ends': CQLTokenType.ENDS,
            'valueset': CQLTokenType.VALUESET,
            'code': CQLTokenType.CODE,
            'concept': CQLTokenType.CONCEPT,
            'codesystem': CQLTokenType.CODESYSTEM
        }
    
    def scan_identifier(self):
        """Override to handle CQL keywords."""
        result = super().scan_identifier()
        
        # Check if identifier is a CQL keyword
        if result.type == TokenType.IDENTIFIER and result.value.lower() in self.cql_keywords:
            # Convert to CQL token type - but we need to handle the enum difference
            # For now, keep as IDENTIFIER but add CQL context
            result.cql_keyword = self.cql_keywords[result.value.lower()]
            
        return result

class CQLParser(FHIRPathParser):
    """
    Clinical Quality Language parser extending FHIRPath parser.
    
    Parses CQL expressions and libraries, extending FHIRPath parser capabilities.
    """
    
    def __init__(self, tokens: List):
        super().__init__(tokens)
        
    def parse_library(self) -> LibraryNode:
        """
        Parse a complete CQL library.
        
        Grammar: library qualifiedIdentifier ('version' versionSpecifier)?
        """
        logger.info("Parsing CQL library")
        
        # Expect 'library' keyword
        if not self.match_keyword('library'):
            raise self.error("Expected 'library' keyword")
        
        # Parse library name (qualified identifier)
        library_name = self.parse_qualified_identifier()
        
        # Optional version
        version = None
        if self.match_keyword('version'):
            version = self.parse_version_specifier()
        
        library = LibraryNode(library_name, version)
        
        # Parse library body (includes, parameters, contexts, definitions)
        while not self.is_at_end():
            if self.match_keyword('include'):
                include_node = self.parse_include()
                library.includes.append(include_node)
            elif self.match_keyword('parameter'):
                param_node = self.parse_parameter()
                library.parameters.append(param_node)
            elif self.match_keyword('context'):
                context_node = self.parse_context()
                library.context = context_node.context_name
            elif self.match_keyword('define'):
                define_node = self.parse_define()
                library.definitions.append(define_node)
            else:
                # Skip unknown constructs for now
                self.advance()
        
        return library
    
    def parse_qualified_identifier(self) -> str:
        """Parse qualified identifier (e.g., 'Common.Demographics')."""
        parts = [self.consume_identifier()]
        
        while self.current_token() and self.current_token().type == TokenType.DOT:
            self.advance()  # consume dot
            parts.append(self.consume_identifier())
        
        return '.'.join(parts)
    
    def parse_version_specifier(self) -> str:
        """Parse version specifier (string literal)."""
        if self.current_token() and self.current_token().type == TokenType.STRING:
            version = self.current_token().value
            self.advance()
            return version
        else:
            raise self.error("Expected version string")
    
    def parse_include(self) -> IncludeNode:
        """Parse include statement."""
        library_name = self.parse_qualified_identifier()
        
        version = None
        if self.match_keyword('version'):
            version = self.parse_version_specifier()
        
        alias = None
        if self.match_keyword('called'):
            alias = self.consume_identifier()
        
        return IncludeNode(library_name, version, alias)
    
    def parse_parameter(self) -> ParameterNode:
        """Parse parameter definition."""
        param_name = self.consume_identifier()
        
        # Optional type
        param_type = None
        if self.current_token() and self.current_token().type == TokenType.IDENTIFIER:
            param_type = self.consume_identifier()
        
        # Optional default value
        default_value = None
        if self.match_keyword('default'):
            default_value = self.parse_expression()
        
        return ParameterNode(param_name, param_type, default_value)
    
    def parse_context(self) -> ContextNode:
        """Parse context definition."""
        context_name = self.consume_identifier()
        return ContextNode(context_name)
    
    def parse_define(self) -> DefineNode:
        """Parse define statement."""
        access_level = "PRIVATE"  # Default
        
        # Check for access level
        if self.match_keyword('public'):
            access_level = "PUBLIC"
        elif self.match_keyword('private'):
            access_level = "PRIVATE"
        
        define_name = self.consume_identifier()
        
        # Expect colon
        if not self.match(TokenType.COLON):
            raise self.error("Expected ':' after define name")
        
        # Parse expression
        expression = self.parse_expression()
        
        return DefineNode(define_name, expression, access_level)
    
    def parse_retrieve(self) -> RetrieveNode:
        """Parse retrieve expression: [ResourceType: terminology]."""
        if not self.match(TokenType.LBRACKET):
            raise self.error("Expected '[' for retrieve")
        
        resource_type = self.consume_identifier()
        
        terminology = None
        if self.match(TokenType.COLON):
            # Parse terminology reference (for now, just string)
            if self.current_token() and self.current_token().type == TokenType.STRING:
                terminology = self.current_token().value
                self.advance()
        
        if not self.match(TokenType.RBRACKET):
            raise self.error("Expected ']' to close retrieve")
        
        return RetrieveNode(resource_type, terminology)
    
    def parse_query_expression(self) -> QueryNode:
        """
        Parse CQL query expression.
        
        Grammar: source_clause with_clause* where_clause? return_clause? sort_clause?
        """
        logger.debug("Parsing CQL query expression")
        
        # Parse source (could be retrieve or identifier)
        source = None
        aliases = []
        
        if self.current_token() and self.current_token().type == TokenType.LBRACKET:
            # Retrieve expression
            source = self.parse_retrieve()
        elif self.current_token() and self.current_token().type == TokenType.IDENTIFIER:
            # Could be identifier or from clause
            if self.current_token().value.lower() == 'from':
                self.advance()  # consume 'from'
                source = self.parse_expression()
                # Check for alias
                if self.current_token() and self.current_token().type == TokenType.IDENTIFIER:
                    aliases.append(self.consume_identifier())
            else:
                source = IdentifierNode(self.consume_identifier())
        
        # Parse optional with clauses
        with_clauses = []
        while self.current_token() and self.current_token().type == TokenType.IDENTIFIER and self.current_token().value.lower() == 'with':
            self.advance()  # consume 'with'
            with_clause = self.parse_with_clause()
            with_clauses.append(with_clause)
        
        # Parse optional where clause
        where_clause = None
        if self.match_keyword('where'):
            where_clause = self.parse_expression()
        
        # Parse optional return clause
        return_clause = None
        if self.match_keyword('return'):
            return_clause = self.parse_expression()
        
        # Parse optional sort clause
        sort_clause = None
        if self.match_keyword('sort'):
            sort_clause = self.parse_sort_clause()
        
        query = QueryNode(source, aliases, where_clause, return_clause, sort_clause)
        
        # Attach with clauses if any
        if with_clauses:
            query.with_clauses = with_clauses
            
        return query
    
    def parse_with_clause(self) -> WithClauseNode:
        """Parse with clause: identifier ':' expression."""
        identifier = self.consume_identifier()
        
        if not self.match(TokenType.COLON):
            raise self.error("Expected ':' in with clause")
        
        expression = self.parse_expression()
        return WithClauseNode(identifier, expression)
    
    def parse_sort_clause(self) -> SortClauseNode:
        """Parse sort clause: 'by' expression ('asc' | 'desc')?."""
        if self.match_keyword('by'):
            expression = self.parse_expression()
            
            direction = "ASC"
            if self.match_keyword('asc'):
                direction = "ASC"
            elif self.match_keyword('desc'):
                direction = "DESC"
            
            return SortClauseNode(expression, direction)
        else:
            raise self.error("Expected 'by' in sort clause")
    
    def parse_clinical_function(self, function_name: str) -> FunctionCallNode:
        """
        Parse CQL clinical functions.
        
        Args:
            function_name: Name of the clinical function
            
        Returns:
            FunctionCallNode for the clinical function
        """
        logger.debug(f"Parsing clinical function: {function_name}")
        
        # Expect opening parenthesis
        if not self.match(TokenType.LPAREN):
            raise self.error(f"Expected '(' after clinical function '{function_name}'")
        
        # Parse arguments
        args = []
        if not self.check(TokenType.RPAREN):
            args.append(self.parse_expression())
            
            while self.match(TokenType.COMMA):
                args.append(self.parse_expression())
        
        if not self.match(TokenType.RPAREN):
            raise self.error("Expected ')' to close clinical function")
        
        return FunctionCallNode(function_name, args)
    
    def parse_temporal_expression(self) -> Any:
        """Parse temporal expressions like 'during', 'overlaps', etc."""
        logger.debug("Parsing temporal expression")
        
        left = self.parse_expression()
        
        # Check for temporal operators
        if self.current_token() and self.current_token().type == TokenType.IDENTIFIER:
            operator = self.current_token().value.lower()
            
            if operator in ['during', 'overlaps', 'before', 'after', 'meets', 'starts', 'ends', 'includes']:
                self.advance()  # consume operator
                right = self.parse_expression()
                
                # Create binary operation node for temporal operation
                return BinaryOpNode(left, operator, right)
        
        return left
    
    def match_keyword(self, keyword: str) -> bool:
        """Check if current token matches a keyword."""
        token = self.current_token()
        if token and token.type == TokenType.IDENTIFIER:
            if token.value.lower() == keyword.lower():
                self.advance()
                return True
        return False
    
    def consume_identifier(self) -> str:
        """Consume and return identifier value."""
        if self.current_token() and self.current_token().type == TokenType.IDENTIFIER:
            value = self.current_token().value
            self.advance()
            return value
        else:
            raise self.error("Expected identifier")
    
    def is_simple_fhirpath_expression(self, expression: str) -> bool:
        """
        Check if expression looks like a simple FHIRPath expression.
        
        Args:
            expression: Expression to check
            
        Returns:
            True if this looks like FHIRPath, False if CQL-specific
        """
        # Phase 1: Simple heuristics
        cql_keywords = ['library', 'define', 'context', 'include', 'parameter']
        cql_constructs = ['[', 'from', 'where', 'return']
        
        # CQL temporal functions - these should be treated as CQL, not FHIRPath
        cql_temporal_functions = ['datetime', 'date', 'time']
        
        # CQL temporal units - expressions containing these are likely CQL arithmetic
        cql_temporal_units = ['year', 'month', 'day', 'hour', 'minute', 'second', 'years', 'months', 'days', 'hours', 'minutes', 'seconds']
        
        expression_lower = expression.lower().strip()
        
        # If it starts with CQL keywords, it's CQL
        for keyword in cql_keywords:
            if expression_lower.startswith(keyword):
                return False
                
        # If it contains CQL-specific constructs, it's CQL
        for construct in cql_constructs:
            if construct in expression_lower:
                return False
        
        # If it contains CQL temporal functions, it's CQL
        for func in cql_temporal_functions:
            if f'{func}(' in expression_lower:
                return False
        
        # If it contains temporal arithmetic (e.g., "+ 1 year", "- 3 months"), it's CQL
        import re
        temporal_arithmetic_pattern = r'[+\-]\s*\d+\s*(' + '|'.join(cql_temporal_units) + r')'
        if re.search(temporal_arithmetic_pattern, expression_lower):
            return False
        
        # If it contains component extraction patterns (e.g., "year from", "month from"), it's CQL
        component_extraction_pattern = r'\b(' + '|'.join(['year', 'month', 'day', 'hour', 'minute', 'second']) + r')\s+from\s+'
        if re.search(component_extraction_pattern, expression_lower):
            return False
        
        # If it contains duration calculation patterns (e.g., "years between", "months between"), it's CQL
        duration_calculation_pattern = r'\b(' + '|'.join(['years', 'months', 'days', 'hours', 'minutes', 'seconds']) + r')\s+between\s+'
        if re.search(duration_calculation_pattern, expression_lower):
            return False
                
        # Otherwise, assume it's FHIRPath
        return True
    
    def parse_expression_or_fhirpath(self, text: str) -> Union[CQLASTNode, ASTNode]:
        """
        Parse expression that could be either CQL or FHIRPath.
        
        Args:
            text: Expression text to parse
            
        Returns:
            AST node (either CQL or FHIRPath)
        """
        # Check if it's a simple FHIRPath expression
        if self.is_simple_fhirpath_expression(text):
            # Use parent FHIRPath parsing
            lexer = FHIRPathLexer(text)
            tokens = lexer.tokenize()
            parser = FHIRPathParser(tokens)
            return parser.parse()
        else:
            # Handle CQL-specific parsing
            if text.strip().startswith('['):
                # This looks like a retrieve expression
                lexer = CQLLexer(text)
                tokens = lexer.tokenize()
                self.tokens = tokens
                self.current = 0
                return self.parse_retrieve()
            else:
                # Check if this contains temporal arithmetic
                import re
                temporal_arithmetic_pattern = r'[+\-]\s*\d+\s*(year|month|day|hour|minute|second|years|months|days|hours|minutes|seconds)'
                if re.search(temporal_arithmetic_pattern, text.lower()):
                    # Parse as CQL temporal arithmetic expression
                    return self.parse_cql_temporal_arithmetic(text)
                
                # Check if this contains component extraction patterns
                component_extraction_pattern = r'\b(year|month|day|hour|minute|second)\s+from\s+'
                if re.search(component_extraction_pattern, text.lower()):
                    # Parse as CQL component extraction expression
                    return self.parse_cql_component_extraction(text)
                
                # Check if this contains duration calculation patterns
                duration_calculation_pattern = r'\b(years|months|days|hours|minutes|seconds)\s+between\s+'
                if re.search(duration_calculation_pattern, text.lower()):
                    # Parse as CQL duration calculation expression
                    return self.parse_cql_duration_calculation(text)
                else:
                    # For other CQL constructs, fall back to FHIRPath for now
                    lexer = FHIRPathLexer(text)
                    tokens = lexer.tokenize()
                    parser = FHIRPathParser(tokens)
                    return parser.parse()
    
    def parse_cql_temporal_arithmetic(self, text: str) -> Union[CQLASTNode, ASTNode]:
        """
        Parse CQL temporal arithmetic expressions like 'DateTime(2023, 1, 15) + 1 year'.
        
        Args:
            text: CQL temporal arithmetic expression
            
        Returns:
            AST node representing the temporal arithmetic operation
        """
        logger.debug(f"Parsing CQL temporal arithmetic: {text}")
        
        # Use regex to split the expression into parts
        import re
        
        # Pattern to match: <expression> +/- <number> <temporal_unit>
        pattern = r'^(.+?)\s*([+\-])\s*(\d+)\s*(year|month|day|hour|minute|second|years|months|days|hours|minutes|seconds)s?$'
        match = re.match(pattern, text.strip(), re.IGNORECASE)
        
        if not match:
            # Fall back to regular FHIRPath parsing
            lexer = FHIRPathLexer(text)
            tokens = lexer.tokenize()
            parser = FHIRPathParser(tokens)
            return parser.parse()
        
        base_expr_text, operator, amount, unit = match.groups()
        
        # Parse the base expression (e.g., "DateTime(2023, 1, 15)")
        base_lexer = FHIRPathLexer(base_expr_text.strip())
        base_tokens = base_lexer.tokenize()
        base_parser = FHIRPathParser(base_tokens)
        base_ast = base_parser.parse()
        
        # Create a special temporal arithmetic node
        # We'll represent this as a function call like "add_years(DateTime(...), 1)"
        unit_lower = unit.lower()
        
        # Normalize unit to singular form
        if unit_lower.endswith('s'):
            unit_lower = unit_lower[:-1]
        
        # Determine the function name based on operator and unit
        if operator == '+':
            func_name = f"add_{unit_lower}s"
        else:  # operator == '-'
            func_name = f"subtract_{unit_lower}s"
        
        # Create arguments: base expression and amount
        amount_literal = LiteralNode(int(amount), 'integer')
        
        # For subtraction, make the amount negative
        if operator == '-':
            amount_literal = LiteralNode(-int(amount), 'integer')
            func_name = f"add_{unit_lower}s"  # Use add with negative number
        
        # Create function call node
        return FunctionCallNode(func_name, [base_ast, amount_literal])
    
    def parse_cql_component_extraction(self, text: str) -> Union[CQLASTNode, ASTNode]:
        """
        Parse CQL component extraction expressions like 'year from DateTime(2023, 6, 15)'.
        
        Args:
            text: CQL component extraction expression
            
        Returns:
            AST node representing the component extraction operation
        """
        logger.debug(f"Parsing CQL component extraction: {text}")
        
        # Use regex to split the expression into parts
        import re
        
        # Pattern to match: <component> from <expression>
        pattern = r'^(year|month|day|hour|minute|second)\s+from\s+(.+)$'
        match = re.match(pattern, text.strip(), re.IGNORECASE)
        
        if not match:
            # Fall back to regular FHIRPath parsing
            lexer = FHIRPathLexer(text)
            tokens = lexer.tokenize()
            parser = FHIRPathParser(tokens)
            return parser.parse()
        
        component, source_expr_text = match.groups()
        
        # Parse the source expression (e.g., "DateTime(2023, 6, 15)")
        source_lexer = FHIRPathLexer(source_expr_text.strip())
        source_tokens = source_lexer.tokenize()
        source_parser = FHIRPathParser(source_tokens)
        source_ast = source_parser.parse()
        
        # Create a component extraction function call
        # e.g., "year from DateTime(...)" becomes "year_from(DateTime(...))"
        func_name = f"{component.lower()}_from"
        
        # Create function call node
        return FunctionCallNode(func_name, [source_ast])
    
    def parse_cql_duration_calculation(self, text: str) -> Union[CQLASTNode, ASTNode]:
        """
        Parse CQL duration calculation expressions like 'years between Date(2020, 1, 1) and Date(2023, 1, 1)'.
        
        Args:
            text: CQL duration calculation expression
            
        Returns:
            AST node representing the duration calculation operation
        """
        logger.debug(f"Parsing CQL duration calculation: {text}")
        
        # Use regex to split the expression into parts
        import re
        
        # Pattern to match: <duration_unit> between <expression1> and <expression2>
        pattern = r'^(years|months|days|hours|minutes|seconds)\s+between\s+(.+?)\s+and\s+(.+)$'
        match = re.match(pattern, text.strip(), re.IGNORECASE)
        
        if not match:
            # Fall back to regular FHIRPath parsing
            lexer = FHIRPathLexer(text)
            tokens = lexer.tokenize()
            parser = FHIRPathParser(tokens)
            return parser.parse()
        
        duration_unit, start_expr_text, end_expr_text = match.groups()
        
        # Parse the start expression (e.g., "Date(2020, 1, 1)")
        start_lexer = FHIRPathLexer(start_expr_text.strip())
        start_tokens = start_lexer.tokenize()
        start_parser = FHIRPathParser(start_tokens)
        start_ast = start_parser.parse()
        
        # Parse the end expression (e.g., "Date(2023, 1, 1)")
        end_lexer = FHIRPathLexer(end_expr_text.strip())
        end_tokens = end_lexer.tokenize()
        end_parser = FHIRPathParser(end_tokens)
        end_ast = end_parser.parse()
        
        # Create a duration calculation function call
        # e.g., "years between Date(...) and Date(...)" becomes "years_between(Date(...), Date(...))"
        func_name = f"{duration_unit.lower()}_between"
        
        # Create function call node with both arguments
        return FunctionCallNode(func_name, [start_ast, end_ast])