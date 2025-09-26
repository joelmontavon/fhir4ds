# Getting Started with FHIR4DS FHIRPath Parser

## Introduction

The FHIR4DS FHIRPath Parser is a comprehensive implementation of the FHIRPath R4 specification, designed to parse FHIRPath expressions into Abstract Syntax Trees (AST) for further processing and evaluation.

## Installation

The FHIRPath parser is included as part of the FHIR4DS package:

```bash
pip install fhir4ds
```

## Quick Start

### Basic Usage

```python
from fhir4ds.parser import parse

# Parse a simple FHIRPath expression
ast = parse("Patient.name")
print(f"Parsed AST: {type(ast).__name__}")
```

### Complete Example

```python
from fhir4ds.parser import parse
from fhir4ds.parser.exceptions import ParseError, LexerError

# Sample FHIR Patient resource
patient = {
    "resourceType": "Patient",
    "name": [
        {
            "given": ["John", "James"],
            "family": "Doe"
        }
    ],
    "telecom": [
        {
            "system": "phone",
            "value": "555-555-5555"
        }
    ]
}

# Parse various FHIRPath expressions
expressions = [
    "Patient.name",
    "Patient.name.given",
    "Patient.name.given.first()",
    "Patient.telecom.where(system = 'phone').value"
]

for expr in expressions:
    try:
        ast = parse(expr)
        print(f"✓ Successfully parsed: {expr}")
    except (ParseError, LexerError) as e:
        print(f"✗ Error parsing '{expr}': {e}")
```

## Core Concepts

### Lexical Analysis

The parser first performs lexical analysis, breaking the FHIRPath expression into tokens:

```python
from fhir4ds.parser.lexer import FHIRPathLexer

expression = "Patient.name.first()"
lexer = FHIRPathLexer(expression)

# Get all tokens
tokens = list(lexer.tokenize())

# Display tokens (excluding EOF)
for token in tokens[:-1]:  # Skip EOF token
    print(f"{token.token_type.name}: '{token.value}'")
```

Output:
```
IDENTIFIER: 'Patient'
DOT: '.'
IDENTIFIER: 'name'
DOT: '.'
IDENTIFIER: 'first'
LPAREN: '('
RPAREN: ')'
```

### Parsing

After tokenization, the parser builds an Abstract Syntax Tree (AST):

```python
from fhir4ds.parser.parser import Parser
from fhir4ds.parser.lexer import FHIRPathLexer

# Tokenize
lexer = FHIRPathLexer("Patient.name")
tokens = list(lexer.tokenize())

# Parse
parser = Parser(tokens)
ast = parser.parse()

print(f"Root node type: {type(ast).__name__}")
```

### AST Structure

The AST consists of various node types representing different parts of the expression:

- **Identifier**: Property names, function names (`Patient`, `name`, `first`)
- **MemberAccess**: Property access operations (`Patient.name`)
- **InvocationExpression**: Function calls (`name.first()`)
- **Literals**: String, number, boolean values
- **BinaryOperation**: Operations with two operands (`age > 18`)

## Working with the AST

### Visualizing the AST

```python
from fhir4ds.parser import parse
from fhir4ds.ast.visitors import ASTPrinter

ast = parse("Patient.name.first()")
printer = ASTPrinter()

print("AST Structure:")
print(printer.visit(ast))
```

### Traversing the AST

You can create custom visitors to traverse and process the AST:

```python
from fhir4ds.ast.visitors import ASTVisitor

class NodeCounter(ASTVisitor):
    def __init__(self):
        self.counts = {}

    def visit(self, node):
        node_type = type(node).__name__
        self.counts[node_type] = self.counts.get(node_type, 0) + 1
        return super().visit(node)

    # Implement required visit methods...
    def visit_identifier(self, node):
        return node.value

    # ... (implement other required methods)

# Usage
ast = parse("Patient.name.first()")
counter = NodeCounter()
counter.visit(ast)
print("Node counts:", counter.counts)
```

## Error Handling

The parser provides detailed error information for debugging:

### Lexer Errors

```python
from fhir4ds.parser import parse
from fhir4ds.parser.exceptions import LexerError

try:
    # Unterminated string
    ast = parse("Patient.name = 'unterminated")
except LexerError as e:
    print(f"Lexer error: {e.message}")
    print(f"Location: line {e.location.line}, column {e.location.column}")
```

### Parser Errors

```python
from fhir4ds.parser import parse
from fhir4ds.parser.exceptions import ParseError

try:
    # Incomplete expression
    ast = parse("Patient.name.")
except ParseError as e:
    print(f"Parse error: {e.message}")
    print(f"Location: line {e.line}, column {e.column}")
```

## Supported FHIRPath Features

The parser supports the full FHIRPath R4 specification, including:

### Path Navigation
- Simple paths: `Patient.name`
- Nested paths: `Patient.name.given`
- Array indexing: `Patient.name[0]`

### Functions
- Collection functions: `first()`, `last()`, `tail()`
- Filtering: `where(condition)`
- Boolean functions: `exists()`, `empty()`

### Operators
- Comparison: `=`, `!=`, `<`, `>`, `<=`, `>=`
- Logical: `and`, `or`, `xor`, `implies`
- Arithmetic: `+`, `-`, `*`, `/`, `mod`

### Literals
- Strings: `'Hello World'`
- Numbers: `42`, `3.14`
- Booleans: `true`, `false`
- Dates: `@2023-01-01`
- Times: `@T12:30:00`

### Advanced Features
- Type operations: `is`, `as`
- Membership: `in`, `contains`
- Polymorphic navigation: `ofType(Type)`

## Best Practices

### Performance
- Reuse lexer instances for similar expressions
- Cache parsed ASTs for frequently used expressions
- Use appropriate error handling for production code

### Error Handling
- Always wrap parsing in try-catch blocks
- Provide meaningful error messages to users
- Log parsing errors for debugging

### Code Organization
- Separate parsing logic from evaluation logic
- Use the visitor pattern for AST processing
- Create reusable parsing utilities

## Next Steps

- **Advanced Integration**: Learn about integrating the parser with FHIR resources
- **Performance Optimization**: Explore performance testing and optimization techniques
- **Custom Processing**: Create custom AST visitors for specific use cases
- **Error Recovery**: Implement robust error handling in production applications

## Examples

See the `examples/parser/` directory for complete, runnable examples:

- `basic_usage.py`: Comprehensive basic usage examples
- `advanced_integration.py`: Advanced integration patterns
- `error_handling_demo.py`: Error handling best practices
- `performance_examples.py`: Performance testing and optimization