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
    
    # CQL literals
    DATE_TIME_LITERAL = "DATE_TIME_LITERAL"  # @2020-01-01
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
    def __init__(self, name: str, parameter_type: Optional['TypeNode'] = None, default_value: Optional[Any] = None):
        self.name = name
        self.parameter_type = parameter_type  # Now supports TypeNode
        self.default_value = default_value

class TypeNode(CQLASTNode):
    """AST node for CQL type expressions including generic types."""
    def __init__(self, base_type: str, generic_types: Optional[List['TypeNode']] = None):
        self.base_type = base_type  # e.g., "Interval", "List", "String"
        self.generic_types = generic_types or []  # e.g., [DateTime] for Interval<DateTime>
    
    def __str__(self):
        if self.generic_types:
            generic_str = ', '.join(str(gt) for gt in self.generic_types)
            return f"{self.base_type}<{generic_str}>"
        return self.base_type

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

class IntervalLiteralNode(CQLASTNode):
    """AST node for CQL interval literals."""
    def __init__(self, start_value: Any, end_value: Any, 
                 start_inclusive: bool = True, end_inclusive: bool = False):
        self.start_value = start_value
        self.end_value = end_value
        self.start_inclusive = start_inclusive
        self.end_inclusive = end_inclusive
    
    def __str__(self):
        start_bracket = '[' if self.start_inclusive else '('
        end_bracket = ']' if self.end_inclusive else ')'
        return f"Interval{start_bracket}{self.start_value}, {self.end_value}{end_bracket}"

class DateTimeLiteralNode(CQLASTNode):
    """AST node for CQL date/time literals."""
    def __init__(self, value: str):
        self.value = value  # The literal value without the @ prefix
    
    def __str__(self):
        return f"@{self.value}"

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
    
    def tokenize(self) -> List:
        """Override tokenization to handle CQL-specific tokens like @ date literals and comments."""
        # Pre-process the expression to handle comments and @ date literals
        processed_expression = self._preprocess_comments(self.expression)
        processed_expression = self._preprocess_cql_literals(processed_expression)
        
        # Create a temporary lexer with the processed expression
        temp_lexer = FHIRPathLexer(processed_expression)
        tokens = temp_lexer.tokenize()
        
        return tokens
    
    def _preprocess_comments(self, expression: str) -> str:
        """Pre-process CQL expression to remove comments."""
        import re
        
        # Remove single-line comments (//.*)
        # Use negative lookbehind to avoid matching / that is part of an operator like /=
        expression = re.sub(r'//.*?(?=\n|$)', '', expression)
        
        # Remove multi-line comments (/* ... */)
        expression = re.sub(r'/\*.*?\*/', '', expression, flags=re.DOTALL)
        
        return expression
    
    def _preprocess_cql_literals(self, expression: str) -> str:
        """Pre-process CQL expression to convert @ date literals to quoted strings."""
        import re
        
        # Pattern to match @ followed by date-like strings
        # @2020-01-01, @2020-01-01T12:00:00, @2020, @2020-01
        date_literal_pattern = r'@(\d{4}(?:-\d{2}(?:-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)?)?)?)'
        
        # Replace @date with "@date" (quoted string)
        def replace_date_literal(match):
            date_value = match.group(1)
            return f'"@{date_value}"'
        
        processed = re.sub(date_literal_pattern, replace_date_literal, expression)
        return processed
    
    def _looks_like_date_after_at(self, token) -> bool:
        """Check if token looks like a date that should follow @."""
        if not token or not hasattr(token, 'value'):
            return False
            
        import re
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',  # @2020-01-01
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$',  # @2020-01-01T12:00:00
            r'^\d{4}$',  # @2020
            r'^\d{4}-\d{2}$',  # @2020-01
        ]
        
        for pattern in date_patterns:
            if re.match(pattern, str(token.value)):
                return True
        
        return False

class CQLParser(FHIRPathParser):
    """
    Clinical Quality Language parser extending FHIRPath parser.
    
    Parses CQL expressions and libraries, extending FHIRPath parser capabilities.
    """
    
    def __init__(self, tokens: List):
        super().__init__(tokens)
    
    def error(self, message: str):
        """Raise a parsing error with current position info."""
        position = getattr(self.current_token, 'position', -1) if self.current_token else -1
        raise ValueError(f"Parse error at position {position}: {message}")
        
    def is_at_end(self) -> bool:
        """Check if we're at the end of the token stream."""
        return self.position >= len(self.tokens)
    
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
        
        while self.current_token and self.current_token.type == TokenType.DOT:
            self.advance()  # consume dot
            parts.append(self.consume_identifier())
        
        return '.'.join(parts)
    
    def parse_version_specifier(self) -> str:
        """Parse version specifier (string literal)."""
        if self.current_token and self.current_token.type == TokenType.STRING:
            version = self.current_token.value
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
        param_name = self.consume_identifier_or_quoted()
        
        # Optional type
        param_type = None
        if self.current_token and self.current_token.type == TokenType.IDENTIFIER:
            param_type = self.parse_type_expression()
        
        # Optional default value
        default_value = None
        if self.match_keyword('default'):
            default_value = self.parse_union_expression()
        
        return ParameterNode(param_name, param_type, default_value)
    
    def parse_type_expression(self) -> TypeNode:
        """
        Parse type expressions including generic types.
        
        Examples:
        - String
        - DateTime
        - Interval<DateTime>
        - List<String>
        """
        logger.debug(f"Parsing type expression at token: {self.current_token}")
        
        # Parse base type
        base_type = self.consume_identifier()
        logger.debug(f"Parsed base type: {base_type}")
        
        # Check for generic type parameters
        generic_types = []
        if (self.current_token and 
            self.current_token.type == TokenType.LESS):
            
            logger.debug("Found '<', parsing generic types")
            self.advance()  # consume '<'
            
            # Parse generic type arguments
            while True:
                logger.debug(f"Parsing generic type argument at token: {self.current_token}")
                generic_types.append(self.parse_type_expression())
                logger.debug(f"Parsed generic type, current token: {self.current_token}")
                
                if (self.current_token and 
                    self.current_token.type == TokenType.COMMA):
                    self.advance()  # consume ','
                    continue
                elif (self.current_token and 
                      self.current_token.type == TokenType.GREATER):
                    self.advance()  # consume '>'
                    logger.debug("Consumed '>', finished parsing generic type")
                    break
                else:
                    raise self.error("Expected ',' or '>' in generic type")
        
        result = TypeNode(base_type, generic_types)
        logger.debug(f"Created TypeNode: {result}")
        return result
    
    def parse_interval_literal(self) -> IntervalLiteralNode:
        """
        Parse interval literal expressions.
        
        Examples:
        - Interval[start, end)  
        - Interval(start, end]
        - Interval[@2023-01-01, @2023-12-31)
        """
        logger.debug(f"Parsing interval literal at token: {self.current_token}")
        
        # Consume 'Interval' keyword
        if not (self.current_token and self.current_token.value == "Interval"):
            raise self.error("Expected 'Interval' keyword")
        self.advance()
        
        # Determine inclusivity from bracket type
        start_inclusive = True
        if self.current_token and self.current_token.type == TokenType.LBRACKET:
            start_inclusive = True
            self.advance()  # consume '['
        elif self.current_token and self.current_token.type == TokenType.LPAREN:
            start_inclusive = False
            self.advance()  # consume '('
        else:
            raise self.error("Expected '[' or '(' after 'Interval'")
        
        # Parse start value
        start_value = self.parse_union_expression()
        
        # Expect comma
        if not (self.current_token and self.current_token.type == TokenType.COMMA):
            raise self.error("Expected ',' in interval literal")
        self.advance()  # consume ','
        
        # Parse end value
        end_value = self.parse_union_expression()
        
        # Determine end inclusivity from bracket type
        end_inclusive = False
        if self.current_token and self.current_token.type == TokenType.RBRACKET:
            end_inclusive = True
            self.advance()  # consume ']'
        elif self.current_token and self.current_token.type == TokenType.RPAREN:
            end_inclusive = False
            self.advance()  # consume ')'
        else:
            raise self.error("Expected ']' or ')' to close interval literal")
        
        result = IntervalLiteralNode(start_value, end_value, start_inclusive, end_inclusive)
        logger.debug(f"Created IntervalLiteralNode: {result}")
        return result
    
    def parse_datetime_literal(self) -> DateTimeLiteralNode:
        """
        Parse date/time literals with @ prefix.
        
        Examples:
        - @2023-01-01
        - @2023-01-01T00:00:00.000Z
        - @T12:30:00
        """
        logger.debug(f"Parsing datetime literal at token: {self.current_token}")
        
        if not (self.current_token and self.current_token.type == TokenType.STRING):
            raise self.error("Expected string token for datetime literal")
        
        value = self.current_token.value
        if not value.startswith('@'):
            raise self.error("DateTime literal must start with '@'")
        
        # Remove the @ prefix
        datetime_value = value[1:]
        self.advance()
        
        result = DateTimeLiteralNode(datetime_value)
        logger.debug(f"Created DateTimeLiteralNode: {result}")
        return result
    
    def parse_quoted_identifier(self) -> ASTNode:
        """
        Parse quoted identifiers as parameter references.
        
        In CQL, quoted strings like "Measurement Period" can be parameter references.
        This creates an IdentifierNode from the FHIRPath parser.
        """
        logger.debug(f"Parsing quoted identifier at token: {self.current_token}")
        
        if not (self.current_token and self.current_token.type == TokenType.STRING):
            raise self.error("Expected string token for quoted identifier")
        
        identifier_name = self.current_token.value
        self.advance()
        
        # Import IdentifierNode from FHIRPath to create parameter reference
        from ...fhirpath.parser.ast_nodes import IdentifierNode
        
        result = IdentifierNode(identifier_name)
        logger.debug(f"Created IdentifierNode from quoted string: {identifier_name}")
        return result
    
    def parse_start_end_expression(self) -> ASTNode:
        """
        Parse CQL start of / end of expressions.
        
        Examples:
        - start of "Measurement Period"
        - end of SomeInterval
        """
        logger.debug(f"Parsing start/end expression at token: {self.current_token}")
        
        # Get the operator (start or end)
        operator = self.current_token.value.lower()
        if operator not in ['start', 'end']:
            raise self.error(f"Expected 'start' or 'end', found {self.current_token.value}")
        
        self.advance()  # consume 'start' or 'end'
        
        # Expect 'of'
        if not (self.current_token and 
                self.current_token.type == TokenType.IDENTIFIER and 
                self.current_token.value.lower() == 'of'):
            raise self.error(f"Expected 'of' after '{operator}', found {self.current_token}")
        
        self.advance()  # consume 'of'
        
        # Parse the operand expression
        operand = self.parse_primary_expression()
        
        # Create a function call node to represent start of / end of
        # This maps to FHIRPath-style function calls like start() and end()
        from ...fhirpath.parser.ast_nodes import FunctionCallNode
        
        result = FunctionCallNode(operator, [operand])
        logger.debug(f"Created FunctionCallNode for {operator} of: {result}")
        return result
    
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
        
        # Define name can be a quoted string or identifier
        if self.current_token and self.current_token.type == TokenType.STRING:
            define_name = self.current_token.value
            self.advance()
        else:
            define_name = self.consume_identifier()
        
        # Expect colon
        if not self.match(TokenType.COLON):
            raise self.error("Expected ':' after define name")
        
        # Parse expression
        expression = self.parse_union_expression()
        
        return DefineNode(define_name, expression, access_level)
    
    def parse_retrieve(self) -> RetrieveNode:
        """Parse retrieve expression: [ResourceType: terminology]."""
        if not self.match(TokenType.LBRACKET):
            raise self.error("Expected '[' for retrieve")
        
        resource_type = self.consume_identifier()
        
        terminology = None
        if self.match(TokenType.COLON):
            # Parse terminology reference (for now, just string)
            if self.current_token and self.current_token.type == TokenType.STRING:
                terminology = self.current_token.value
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
        
        if self.current_token and self.current_token.type == TokenType.LBRACKET:
            # Retrieve expression
            source = self.parse_retrieve()
            # Check for alias after retrieve
            if self.current_token and self.current_token.type == TokenType.IDENTIFIER:
                aliases.append(self.consume_identifier())
        elif self.current_token and self.current_token.type == TokenType.IDENTIFIER:
            # Could be identifier or from clause
            if self.current_token.value.lower() == 'from':
                self.advance()  # consume 'from'
                source = self.parse_union_expression()
                # Check for alias
                if self.current_token and self.current_token.type == TokenType.IDENTIFIER:
                    aliases.append(self.consume_identifier())
            else:
                source = IdentifierNode(self.consume_identifier())
        
        # Parse optional with clauses
        with_clauses = []
        while self.current_token and self.current_token.type == TokenType.IDENTIFIER and self.current_token.value.lower() == 'with':
            self.advance()  # consume 'with'
            with_clause = self.parse_with_clause()
            with_clauses.append(with_clause)
        
        # Parse optional where clause
        where_clause = None
        if self.match_keyword('where'):
            where_clause = self.parse_union_expression()
        
        # Parse optional return clause
        return_clause = None
        if self.match_keyword('return'):
            return_clause = self.parse_union_expression()
        
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
        
        expression = self.parse_union_expression()
        return WithClauseNode(identifier, expression)
    
    def parse_sort_clause(self) -> SortClauseNode:
        """Parse sort clause: 'by' expression ('asc' | 'desc')?."""
        if self.match_keyword('by'):
            expression = self.parse_union_expression()
            
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
            args.append(self.parse_union_expression())
            
            while self.match(TokenType.COMMA):
                args.append(self.parse_union_expression())
        
        if not self.match(TokenType.RPAREN):
            raise self.error("Expected ')' to close clinical function")
        
        return FunctionCallNode(function_name, args)
    
    def parse_temporal_expression(self) -> Any:
        """Parse temporal expressions like 'during', 'overlaps', etc."""
        logger.debug("Parsing temporal expression")
        
        left = self.parse_union_expression()
        
        # Check for temporal operators
        if self.current_token and self.current_token.type == TokenType.IDENTIFIER:
            operator = self.current_token.value.lower()
            
            if operator in ['during', 'overlaps', 'before', 'after', 'meets', 'starts', 'ends', 'includes']:
                self.advance()  # consume operator
                right = self.parse_union_expression()
                
                # Create binary operation node for temporal operation
                return BinaryOpNode(left, operator, right)
        
        return left
    
    def match(self, token_type: TokenType) -> bool:
        """Check if current token matches the specified type and advance if it does."""
        if self.current_token and self.current_token.type == token_type:
            self.advance()
            return True
        return False
    
    def match_keyword(self, keyword: str) -> bool:
        """Check if current token matches a keyword."""
        token = self.current_token
        if token and token.type == TokenType.IDENTIFIER:
            if token.value.lower() == keyword.lower():
                self.advance()
                return True
        return False
    
    def consume_identifier(self) -> str:
        """Consume and return identifier value."""
        if self.current_token and self.current_token.type == TokenType.IDENTIFIER:
            value = self.current_token.value
            self.advance()
            return value
        else:
            raise self.error("Expected identifier")
    
    def consume_identifier_or_quoted(self) -> str:
        """
        Consume and return identifier value, supporting both regular identifiers
        and quoted identifiers (strings).
        
        CQL allows parameter names to be quoted strings like "Measurement Period".
        """
        if self.current_token:
            if self.current_token.type == TokenType.IDENTIFIER:
                value = self.current_token.value
                self.advance()
                return value
            elif self.current_token.type == TokenType.STRING:
                # CQL allows quoted identifiers for parameter names
                value = self.current_token.value
                self.advance()
                return value
        
        raise self.error("Expected identifier or quoted identifier")
    
    def parse_primary_expression(self) -> ASTNode:
        """Override FHIRPath primary expression parsing to handle CQL constructs."""
        # Check for CQL resource retrieval syntax [ResourceType]
        if self.current_token and self.current_token.type == TokenType.LBRACKET:
            return self.parse_retrieve()
        
        # Check for DateTime literals with @ prefix
        elif (self.current_token and 
              self.current_token.type == TokenType.STRING and
              self.current_token.value.startswith('@')):
            return self.parse_datetime_literal()
            
        # Check for quoted identifiers (parameter references)
        elif (self.current_token and 
              self.current_token.type == TokenType.STRING):
            # In CQL, quoted strings can be parameter references like "Measurement Period"
            return self.parse_quoted_identifier()
            
        # Check for Interval literal syntax: Interval[...] or Interval(...)
        elif (self.current_token and 
              self.current_token.type == TokenType.IDENTIFIER and
              self.current_token.value == "Interval" and
              self.position + 1 < len(self.tokens) and
              self.tokens[self.position + 1].type in [TokenType.LBRACKET, TokenType.LPAREN]):
            return self.parse_interval_literal()
            
        # Check for CQL start/end operators (start of, end of)
        elif (self.current_token and 
              self.current_token.type == TokenType.IDENTIFIER and
              self.current_token.value.lower() in ['start', 'end'] and
              self.position + 1 < len(self.tokens) and
              self.tokens[self.position + 1].type == TokenType.IDENTIFIER and
              self.tokens[self.position + 1].value.lower() == 'of'):
            return self.parse_start_end_expression()
            
        # Check for function calls (including CQL functions like AgeInYearsAt)
        elif (self.current_token and 
              self.current_token.type == TokenType.IDENTIFIER):
            
            # Look ahead to see if this is a function call
            name = self.current_token.value
            if (self.position + 1 < len(self.tokens) and 
                self.tokens[self.position + 1].type == TokenType.LPAREN):
                
                # Handle ALL function calls with CQL-aware argument parsing
                return self.parse_cql_function_call()
        
        # Fall back to parent FHIRPath parsing for other cases
        return super().parse_primary_expression()
    
    def parse_cql_function_call(self) -> FunctionCallNode:
        """Parse CQL function calls that can take query arguments."""
        name = self.current_token.value
        self.advance()  # Skip function name
        
        if self.current_token.type != TokenType.LPAREN:
            raise self.error("Expected '(' after function name")
        
        self.advance()  # Skip '('
        args = []
        
        if self.current_token.type != TokenType.RPAREN:
            # Parse first argument - might be a CQL query
            args.append(self.parse_cql_function_argument())
            
            # Parse additional arguments
            while self.current_token.type == TokenType.COMMA:
                self.advance()  # Skip ','
                args.append(self.parse_cql_function_argument())
        
        if self.current_token.type != TokenType.RPAREN:
            raise self.error(f"Expected ')' after function arguments, found {self.current_token}")
        
        self.advance()  # Skip ')'
        return FunctionCallNode(name, args)
    
    def parse_cql_function_argument(self) -> ASTNode:
        """Parse function argument that might be a CQL query."""
        # Check if argument starts with [ResourceType] - this is a CQL query
        if (self.current_token and 
            self.current_token.type == TokenType.LBRACKET):
            return self.parse_query_expression()
        else:
            # Parse as regular FHIRPath expression
            return self.parse_or_expression()
    
    def _is_cql_function_call_with_cql_args(self, text: str) -> bool:
        """
        Check if this is a CQL function call with CQL expressions as arguments.
        
        Examples: Exists([Patient] P), Count([Patient] P where ...), First([Patient] P sort by ...)
        """
        import re
        
        # Pattern for CQL functions that take CQL query expressions
        cql_function_pattern = r'^(Exists|Count|First|Last|Sum|Max|Min|Avg|StdDev|Median|Mode)\s*\(\s*\['
        return bool(re.search(cql_function_pattern, text.strip(), re.IGNORECASE))
    
    def _parse_cql_function_call(self, text: str) -> FunctionCallNode:
        """
        Parse CQL function calls like Exists([Patient] P), Count([Patient] P where ...).
        """
        import re
        
        # Extract function name and arguments
        match = re.match(r'^(\w+)\s*\(\s*(.*)\s*\)$', text.strip(), re.DOTALL)
        if not match:
            raise ValueError(f"Invalid CQL function call format: {text}")
        
        function_name = match.group(1)
        args_text = match.group(2)
        
        # Parse the arguments as CQL expressions
        args = []
        if args_text.strip():
            # For now, assume single argument that's a CQL query
            # Parse the argument as a CQL query expression
            lexer = CQLLexer(args_text)
            tokens = lexer.tokenize()
            self.tokens = tokens
            self.position = 0
            self.current_token = tokens[0] if tokens else None
            
            arg_node = self.parse_query_expression()
            args.append(arg_node)
        
        return FunctionCallNode(function_name, args)
    
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
        
        # If it contains @ date literals (CQL-specific syntax), it's CQL
        if '@' in expression:
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
                # This looks like a CQL query expression with retrieve
                lexer = CQLLexer(text)
                tokens = lexer.tokenize()
                self.tokens = tokens
                self.position = 0
                self.current_token = tokens[0] if tokens else None
                return self.parse_query_expression()
            elif self._is_cql_function_call_with_cql_args(text):
                # Handle CQL functions like Exists([Patient] P), Count([Patient] P where ...)
                return self._parse_cql_function_call(text)
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
                
                # Check if this contains statistical function with resource query patterns
                statistical_function_pattern = r'\b(stddev|stdev|variance|median|mode|percentile|count|sum|avg|average|min|max)\s*\(\s*\[.*?\].*?\)'
                if re.search(statistical_function_pattern, text.lower(), re.DOTALL):
                    # Parse as CQL statistical function with resource query
                    return self.parse_cql_statistical_function(text)
                
                # Check if this contains define statements
                define_pattern = r'define\s+"[^"]+"\s*:'
                if re.search(define_pattern, text.lower()):
                    # Parse as complete CQL library/define construct
                    return self.parse_cql_define_construct(text)
                
                # Check if this contains resource queries with sorting/clauses
                resource_query_pattern = r'\[[^\]]+\]\s+\w+\s+(where|return|sort\s+by)'
                if re.search(resource_query_pattern, text.lower()):
                    # Parse as CQL resource query with clauses
                    return self.parse_cql_resource_query(text)
                
                # For other CQL constructs, try full CQL parsing instead of FHIRPath
                try:
                    lexer = CQLLexer(text)
                    tokens = lexer.tokenize()
                    self.tokens = tokens
                    self.position = 0
                    self.current_token = tokens[0] if tokens else None
                    # Try to parse as CQL expression first
                    return self.parse_union_expression()
                except Exception:
                    # If CQL parsing fails, fall back to FHIRPath
                    lexer = FHIRPathLexer(text)
                    tokens = lexer.tokenize()
                    parser = FHIRPathParser(tokens)
                    return parser.parse()
    
    def parse_cql_statistical_function(self, text: str) -> Union[CQLASTNode, ASTNode]:
        """
        Parse CQL statistical function expressions with resource queries.
        
        Handles patterns like:
        - StdDev([Observation: "Systolic Blood Pressure"] O return O.valueQuantity.value)
        - Count([Patient] P where P.active = true)
        
        Args:
            text: CQL statistical function expression
            
        Returns:
            FunctionCallNode with proper resource query handling
        """
        import re
        
        # Extract function name and arguments
        func_pattern = r'(\w+)\s*\(\s*(.*)\s*\)'
        match = re.match(func_pattern, text.strip(), re.DOTALL)
        if not match:
            # Fallback to FHIRPath parsing
            lexer = FHIRPathLexer(text)
            tokens = lexer.tokenize()
            parser = FHIRPathParser(tokens)
            return parser.parse()
        
        func_name = match.group(1)
        func_args = match.group(2).strip()
        
        # Parse the resource query argument
        try:
            # Create a temporary parser for the resource query
            lexer = CQLLexer(func_args)
            tokens = lexer.tokenize()
            temp_parser = CQLParser(tokens)
            
            # Try to parse as query expression
            query_ast = temp_parser.parse_query_expression()
            
            # Create function call node with parsed query
            return FunctionCallNode(func_name, [query_ast])
            
        except Exception as e:
            logger.debug(f"Failed to parse statistical function arguments: {e}")
            # Fallback: create simple function call with raw arguments
            return FunctionCallNode(func_name, [LiteralNode(func_args, "string")])
    
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
    
    def parse_cql_define_construct(self, text: str) -> Union[CQLASTNode, ASTNode]:
        """
        Parse CQL define statements and complete constructs.
        
        Args:
            text: CQL define construct text
            
        Returns:
            DefineNode or appropriate AST structure
        """
        logger.debug(f"Parsing CQL define construct: {text[:100]}...")
        
        try:
            # Create CQL lexer and parser for the full construct
            lexer = CQLLexer(text)
            tokens = lexer.tokenize()
            self.tokens = tokens
            self.position = 0
            self.current_token = tokens[0] if tokens else None
            
            # Check if this is a complete library or just a define statement
            if text.strip().lower().startswith('library'):
                return self.parse_library()
            elif text.strip().lower().startswith('define'):
                # Parse define statement - consume the 'define' keyword first
                if self.match_keyword('define'):
                    return self.parse_define()
                else:
                    raise self.error("Expected 'define' keyword")
            else:
                # Try to parse as general CQL expression
                return self.parse_union_expression()
                
        except Exception as e:
            logger.warning(f"CQL define parsing failed: {e}, falling back to FHIRPath")
            import traceback
            logger.debug(f"Full traceback: {traceback.format_exc()}")
            # Fall back to FHIRPath parsing
            lexer = FHIRPathLexer(text)
            tokens = lexer.tokenize()
            parser = FHIRPathParser(tokens)
            return parser.parse()
    
    def parse_cql_resource_query(self, text: str) -> Union[CQLASTNode, ASTNode]:
        """
        Parse CQL resource queries with where/return/sort clauses.
        
        Args:
            text: CQL resource query text
            
        Returns:
            QueryNode or appropriate AST structure
        """
        logger.debug(f"Parsing CQL resource query: {text[:100]}...")
        
        try:
            # Create CQL lexer and parser for the resource query
            lexer = CQLLexer(text)
            tokens = lexer.tokenize()
            self.tokens = tokens
            self.position = 0
            self.current_token = tokens[0] if tokens else None
            
            # Parse as query expression
            return self.parse_query_expression()
            
        except Exception as e:
            logger.debug(f"CQL resource query parsing failed: {e}, falling back to FHIRPath")
            # Fall back to FHIRPath parsing
            lexer = FHIRPathLexer(text)
            tokens = lexer.tokenize()
            parser = FHIRPathParser(tokens)
            return parser.parse()