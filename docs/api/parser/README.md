# FHIRPath Parser API Reference

## Overview

The FHIR4DS FHIRPath Parser provides a complete implementation of the FHIRPath R4 specification, converting FHIRPath expressions into Abstract Syntax Trees (AST) for further processing.

## Core Components

### Parser Classes

#### `FHIRPathLexer`
The lexer tokenizes FHIRPath expressions into a sequence of tokens.

```python
from fhir4ds.parser.lexer import FHIRPathLexer

lexer = FHIRPathLexer("Patient.name.given.first()")
tokens = list(lexer.tokenize())
```

**Methods:**
- `tokenize() -> Generator[Token, None, None]`: Tokenizes the expression and returns a generator of Token objects.

**Raises:**
- `LexerError`: When encountering invalid characters or unterminated strings.

#### `Parser`
The parser converts tokens into an Abstract Syntax Tree.

```python
from fhir4ds.parser.parser import Parser
from fhir4ds.parser.lexer import FHIRPathLexer

lexer = FHIRPathLexer("Patient.name")
tokens = list(lexer.tokenize())
parser = Parser(tokens)
ast = parser.parse()
```

**Methods:**
- `parse() -> FHIRPathNode`: Parses the tokens and returns the root AST node.

**Raises:**
- `ParseError`: When encountering syntax errors in the expression.

#### High-Level Interface

#### `parse(expression: str, resource: dict = None) -> FHIRPathNode`
High-level parsing function that combines lexing and parsing.

```python
from fhir4ds.parser import parse

ast = parse("Patient.name.given.first()")
```

**Parameters:**
- `expression`: The FHIRPath expression string to parse
- `resource`: Optional FHIR resource context (currently unused)

**Returns:**
- `FHIRPathNode`: The root node of the parsed AST

**Raises:**
- `LexerError`: For tokenization errors
- `ParseError`: For parsing errors

## Token Types

The lexer recognizes the following token types:

### Literals
- `STRING_LITERAL`: String values enclosed in single quotes
- `INTEGER_LITERAL`: Whole numbers
- `DECIMAL_LITERAL`: Decimal numbers
- `BOOLEAN_LITERAL`: true/false values
- `DATETIME_LITERAL`: DateTime values with @ prefix
- `TIME_LITERAL`: Time values with @T prefix
- `QUANTITY_LITERAL`: Numeric values with units

### Identifiers
- `IDENTIFIER`: Property names, function names, and keywords

### Keywords
- `AND`, `OR`, `XOR`, `IMPLIES`: Logical operators
- `NOT`: Negation operator
- `IS`, `AS`: Type operators
- `IN`, `CONTAINS`: Membership operators
- `MOD`: Modulo operator

### Operators
- `EQUAL` (=), `NOT_EQUAL` (!=): Equality operators
- `LESS_THAN` (<), `LESS_EQUAL` (<=): Comparison operators
- `GREATER_THAN` (>), `GREATER_EQUAL` (>=): Comparison operators
- `PLUS` (+), `MINUS` (-): Arithmetic operators
- `MULTIPLY` (*), `DIVIDE` (/): Arithmetic operators

### Delimiters
- `DOT` (.): Member access
- `LPAREN` ((), `RPAREN` ()): Function calls and grouping
- `LBRACKET` ([), `RBRACKET` (]): Array indexing
- `COMMA` (,): Parameter separation

## AST Node Types

### Base Types
- `FHIRPathNode`: Abstract base class for all AST nodes
- `Literal`: Base class for literal values
- `Expression`: Base class for expressions

### Specific Node Types
- `Identifier`: Property and function names
- `StringLiteral`, `NumberLiteral`, `BooleanLiteral`: Literal values
- `BinaryOperation`: Binary operations with left/right operands
- `UnaryOperation`: Unary operations with single operand
- `FunctionCall`: Function invocations
- `MemberAccess`: Property access (e.g., Patient.name)
- `InvocationExpression`: Method calls (e.g., name.first())
- `Indexer`: Array/collection indexing

## Error Handling

### Exception Hierarchy
- `LexerError`: Errors during tokenization
- `ParseError`: Errors during parsing
- `ValidationError`: Semantic validation errors

### Error Information
All errors include:
- `message`: Descriptive error message
- `location`: Source location with line and column information
- Context information for debugging

## Examples

### Basic Parsing
```python
from fhir4ds.parser import parse

# Simple property access
ast = parse("Patient.name")

# Function call
ast = parse("Patient.name.given.first()")

# Complex expression
ast = parse("Patient.telecom.where(system = 'phone').value")
```

### Error Handling
```python
from fhir4ds.parser import parse
from fhir4ds.parser.exceptions import ParseError, LexerError

try:
    ast = parse("Patient.name.")  # Invalid syntax
except ParseError as e:
    print(f"Parse error at line {e.line}, column {e.column}: {e.message}")
except LexerError as e:
    print(f"Lexer error: {e.message}")
```

### Working with AST
```python
from fhir4ds.parser import parse
from fhir4ds.ast.visitors import ASTPrinter

ast = parse("Patient.name.first()")
printer = ASTPrinter()
print(printer.visit(ast))
```