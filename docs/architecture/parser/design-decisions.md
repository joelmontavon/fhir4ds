# FHIRPath Parser Architecture and Design Decisions

## Overview

The FHIR4DS FHIRPath Parser is designed as a robust, extensible, and specification-compliant implementation of the FHIRPath R4 standard. This document outlines the key architectural decisions and design principles that guide the implementation.

## Architecture Principles

### 1. Specification Compliance First

**Decision**: Prioritize 100% compliance with the FHIRPath R4 specification over implementation convenience.

**Rationale**:
- Ensures interoperability with other FHIRPath implementations
- Provides predictable behavior for users familiar with the specification
- Enables validation against official test suites

**Implementation**:
- All grammar rules strictly follow the FHIRPath specification
- Token types map directly to specification terminology
- Error handling aligns with specification requirements

### 2. Separation of Concerns

**Decision**: Separate lexical analysis, parsing, and semantic analysis into distinct phases.

**Rationale**:
- Modularity enables independent testing and optimization
- Clear boundaries make the codebase maintainable
- Follows established compiler design patterns

**Architecture**:
```
FHIRPath Expression
        ↓
    Lexer (Tokenization)
        ↓
    Parser (AST Generation)
        ↓
    Semantic Analysis (Validation)
        ↓
    AST with Metadata
```

### 3. Rich Error Information

**Decision**: Provide detailed error information including source location and context.

**Rationale**:
- Essential for debugging complex FHIRPath expressions
- Improves developer experience significantly
- Enables better tooling and IDE integration

**Implementation**:
- All tokens include precise source location (line, column, offset)
- Errors include contextual information and suggestions
- Error hierarchy supports different error types

## Component Design

### Lexer Design

#### Token-Based Architecture
**Decision**: Use a comprehensive token type system covering all FHIRPath constructs.

**Benefits**:
- Complete coverage of FHIRPath syntax
- Efficient parsing through clear token boundaries
- Support for complex literals (dates, quantities, etc.)

#### Regex-Based Tokenization
**Decision**: Use a single comprehensive regex for tokenization efficiency.

**Rationale**:
- High performance through single-pass tokenization
- Comprehensive pattern matching for all token types
- Proper handling of operator precedence in tokenization

#### Error Recovery
**Decision**: Fail fast with detailed error information rather than attempting recovery.

**Rationale**:
- FHIRPath expressions are typically short and precise
- Clear error messages more valuable than partial parsing
- Prevents incorrect AST generation from malformed input

### Parser Design

#### Recursive Descent with Operator Precedence
**Decision**: Implement a recursive descent parser with explicit operator precedence handling.

**Rationale**:
- Maps naturally to FHIRPath grammar structure
- Provides clear, readable parsing logic
- Handles operator precedence correctly and explicitly

#### AST-First Approach
**Decision**: Generate rich AST structures rather than immediate evaluation.

**Benefits**:
- Enables multiple processing passes (optimization, analysis, etc.)
- Supports complex transformations and code generation
- Allows for debugging and visualization tools

#### Immutable AST Nodes
**Decision**: Make all AST nodes immutable (frozen dataclasses).

**Rationale**:
- Prevents accidental modification during processing
- Enables safe parallel processing and caching
- Reduces debugging complexity

### AST Design

#### Visitor Pattern Implementation
**Decision**: Use the visitor pattern for AST traversal and processing.

**Benefits**:
- Separation of traversal logic from node structure
- Extensibility for new processing types
- Type safety through generic visitor interfaces

#### Metadata Integration
**Decision**: Include population-scale metadata in every AST node.

**Rationale**:
- Supports advanced optimization for population queries
- Enables performance analysis and optimization
- Provides context for semantic analysis

#### Rich Node Hierarchy
**Decision**: Create specific node types for different language constructs.

**Benefits**:
- Type safety in processing code
- Clear semantic meaning for each construct
- Enables construct-specific optimizations

## Error Handling Strategy

### Exception Hierarchy
```
FHIRPathError (base)
├── LexerError (tokenization errors)
├── ParseError (syntax errors)
└── ValidationError (semantic errors)
```

### Error Context
**Decision**: Include comprehensive error context in all exceptions.

**Information Included**:
- Precise source location (line, column, offset)
- Contextual information about the error
- Suggestions for correction where possible
- Original expression text for reference

### Error Recovery Philosophy
**Decision**: Prioritize error clarity over error recovery.

**Rationale**:
- FHIRPath expressions are typically written by developers, not end users
- Clear error messages enable quick fixes
- Partial parsing results can be misleading

## Performance Considerations

### Lexer Optimization
- Single-pass tokenization using compiled regex
- Lazy token generation through generators
- Efficient string handling with minimal copying

### Parser Optimization
- Recursive descent with minimal backtracking
- Direct AST construction without intermediate representations
- Operator precedence handling without parser state

### Memory Management
- Immutable structures enable sharing and caching
- Metadata stored efficiently in dataclass structures
- Generator-based token stream reduces memory usage

## Extensibility Points

### Custom Token Types
The lexer can be extended with additional token types for domain-specific extensions:

```python
class CustomTokenType(Enum):
    CUSTOM_LITERAL = auto()

# Extend token specification
_TOKEN_SPECIFICATION.append(('CUSTOM_LITERAL', r'custom_pattern'))
```

### Custom AST Nodes
New AST node types can be added by extending the base `FHIRPathNode`:

```python
@dataclass(frozen=True)
class CustomExpression(Expression):
    custom_property: str

    def accept(self, visitor):
        return visitor.visit_custom_expression(self)
```

### Custom Visitors
New processing logic can be added through custom visitors:

```python
class CustomProcessor(ASTVisitor[CustomResult]):
    def visit_custom_expression(self, node):
        # Custom processing logic
        return CustomResult()
```

## Testing Strategy

### Specification Compliance
- Official FHIRPath test suite integration
- Comprehensive test coverage for all language features
- Regression testing for specification updates

### Error Handling Testing
- Systematic testing of all error conditions
- Verification of error message quality and accuracy
- Testing of error location precision

### Performance Testing
- Benchmarking against reference implementations
- Memory usage profiling and optimization
- Scalability testing with complex expressions

## Future Considerations

### Language Extensions
The architecture supports future FHIRPath language extensions:
- Additional operators and functions
- Domain-specific literal types
- Custom navigation patterns

### Optimization Opportunities
- AST caching for repeated expressions
- Compilation to intermediate representations
- JIT compilation for high-frequency expressions

### Integration Points
- Code generation for other languages
- Integration with FHIR validation frameworks
- Support for FHIRPath-based query languages

## Alternative Approaches Considered

### Parser Generators
**Considered**: Using ANTLR or similar parser generators.

**Decision**: Hand-written recursive descent parser.

**Rationale**:
- Better error messages and control
- No external dependencies
- Clearer debugging and maintenance

### Immediate Evaluation
**Considered**: Evaluating expressions during parsing.

**Decision**: AST generation with separate evaluation phase.

**Rationale**:
- Better separation of concerns
- Enables multiple evaluation strategies
- Supports code analysis and transformation tools

### Mutable AST
**Considered**: Mutable AST nodes for in-place modification.

**Decision**: Immutable AST nodes.

**Rationale**:
- Thread safety and parallel processing
- Prevents accidental modifications
- Enables safe caching and sharing

## Conclusion

The FHIRPath parser architecture balances specification compliance, performance, and extensibility. The design decisions prioritize developer experience through clear error messages, maintain high performance through efficient algorithms, and provide extensibility for future enhancements while maintaining backward compatibility.