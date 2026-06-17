# PEP Draft 001: FHIRPath Parser and AST Foundation

```
PEP: 001
Title: FHIRPath Parser and AST Implementation
Author: Senior Solution Architect/Engineer
Status: Implemented
Type: Standard
Created: 25-01-2025
Updated: 26-01-2025
Completed: 26-01-2025
Version: 1.0
Implementation Summary: project-docs/peps/implemented/pep-001-completion-summary.md
```

---

## Abstract

This PEP proposes implementing a complete FHIRPath parser and Abstract Syntax Tree (AST) as the foundational layer for the unified FHIR4DS architecture. The parser will handle the complete FHIRPath R4 grammar, producing a well-structured AST suitable for CTE generation and population-scale optimization. This component serves as the critical foundation upon which all other specifications (SQL-on-FHIR, CQL) will be built through translation to FHIRPath expressions. The implementation will prioritize correctness, performance, and extensibility while maintaining clean separation from execution concerns.

## Motivation

### Why Start with FHIRPath Parser

FHIRPath is the foundational expression language in healthcare interoperability:

1. **FHIRPath R4 Specification**: The base standard that other specifications extend
2. **Translation Target**: SQL-on-FHIR and CQL expressions translate to FHIRPath
3. **Single Point of Correctness**: All expression parsing goes through one validated system
4. **Testable Foundation**: 934 official test cases provide clear validation criteria

### Current State Problems

The existing FHIRPath implementation has severe limitations:
- **0.9% compliance** (8/934 tests passing) with FHIRPath R4 specification
- Mixed parsing logic scattered across multiple components
- No clear AST representation for optimization
- Cannot serve as foundation for other specifications

### Expected Benefits

1. **100% FHIRPath R4 Compliance**: Target all 934 official test cases
2. **Clean Architecture Foundation**: Well-structured AST for all subsequent layers
3. **Performance Optimization Ready**: AST designed for population-scale transformations
4. **Specification Translation Ready**: Clear target for SQL-on-FHIR and CQL translation

## Rationale

### Design Principles

- **Complete Grammar Coverage**: Support entire FHIRPath R4 specification
- **Clean AST Design**: Structured representation suitable for analysis and transformation
- **Population-Scale Ready**: AST nodes designed for batch/population processing
- **Separation of Concerns**: Pure parsing with no execution logic
- **Extensibility**: AST structure accommodates future specification features

### Why Focus Only on Parser/AST

1. **Foundation First**: Everything else depends on correct parsing
2. **Testable Isolation**: Can validate against official test cases independently
3. **Clear Success Criteria**: 934 test cases provide unambiguous validation
4. **Manageable Scope**: Single, focused component with clear boundaries

### AST Design Philosophy

The AST will represent FHIRPath expressions as structured data suitable for:
- **Analysis**: Understanding expression complexity and dependencies
- **Optimization**: Identifying population-scale optimization opportunities
- **Translation**: Converting to SQL CTE chains
- **Validation**: Ensuring semantic correctness

## Specification

### FHIRPath Grammar Support

Complete implementation of FHIRPath R4 specification including:

#### Core Path Navigation
- **Simple paths**: `Patient.name`
- **Array indexing**: `Patient.name[0]`
- **Nested navigation**: `Patient.name.given.first()`

#### Functions and Operations
- **Collection functions**: `first()`, `last()`, `tail()`, `take()`, `skip()`
- **Filtering**: `where()`, `select()`, `exists()`
- **Boolean operations**: `and`, `or`, `not`
- **Comparison operations**: `=`, `!=`, `<`, `>`, `<=`, `>=`
- **Math operations**: `+`, `-`, `*`, `/`, `mod`

#### Data Types and Literals
- **Primitives**: strings, numbers, booleans, dates
- **Collections**: arrays and sets
- **FHIR types**: Quantity, CodeableConcept, etc.

#### Advanced Features
- **Polymorphic navigation**: `ofType()`, `as()`
- **Aggregation**: `count()`, `sum()`, `avg()`
- **String functions**: `startsWith()`, `contains()`, `matches()`

### AST Node Structure

```python
@dataclass
class FHIRPathAST:
    """Root AST node for FHIRPath expressions."""
    expression: FHIRPathNode
    source_text: str

@dataclass
class FHIRPathNode:
    """Base class for all AST nodes."""
    node_type: str
    source_location: SourceLocation

@dataclass
class PathExpression(FHIRPathNode):
    """Navigation path like 'Patient.name.given'."""
    steps: List[PathStep]

@dataclass
class FunctionCall(FHIRPathNode):
    """Function call like 'first()' or 'where(condition)'."""
    function_name: str
    arguments: List[FHIRPathNode]

@dataclass
class BinaryOperation(FHIRPathNode):
    """Binary operations like 'and', 'or', '='."""
    operator: str
    left: FHIRPathNode
    right: FHIRPathNode

@dataclass
class Literal(FHIRPathNode):
    """Literal values like strings, numbers, booleans."""
    value: Any
    data_type: str
```

### Parser Architecture

```python
class FHIRPathParser:
    """Complete FHIRPath R4 parser implementation."""

    def parse(self, expression: str) -> FHIRPathAST:
        """Parse FHIRPath expression into AST."""

    def validate(self, ast: FHIRPathAST) -> List[ValidationError]:
        """Validate AST for semantic correctness."""

class FHIRPathLexer:
    """Tokenizer for FHIRPath expressions."""

    def tokenize(self, expression: str) -> List[Token]:
        """Convert expression string into tokens."""

class ASTValidator:
    """Semantic validation for FHIRPath AST."""

    def validate(self, ast: FHIRPathAST) -> ValidationResult:
        """Ensure AST represents valid FHIRPath expression."""
```

### Error Handling

```python
@dataclass
class ParseError:
    """Parser error with location and context."""
    message: str
    location: SourceLocation
    context: str

@dataclass
class ValidationError:
    """Semantic validation error."""
    message: str
    node: FHIRPathNode
    error_type: str
```

## Implementation

### Development Phases

#### Phase 1: Grammar Foundation (Week 1)
- FHIRPath lexer with complete token recognition
- Basic parser structure and error handling
- Core AST node classes and validation

#### Phase 2: Core Parsing (Week 2)
- Path navigation parsing (`Patient.name.given`)
- Function call parsing (`first()`, `where()`)
- Literal parsing (strings, numbers, booleans)

#### Phase 3: Complete Grammar (Week 3)
- Binary operations (`and`, `or`, comparisons)
- Complex expressions and nested structures
- Advanced functions and polymorphic operations

#### Phase 4: Validation and Testing (Week 4)
- Comprehensive test suite against 934 official test cases
- AST validation and semantic checking
- Error handling and reporting refinement

### Success Criteria

**Primary Goal**: 100% parsing success rate on FHIRPath R4 official test cases
- All 934 test expressions parse successfully
- Generated AST accurately represents expression semantics
- Error handling provides clear, actionable messages

**Secondary Goals**:
- Performance: Parse complex expressions <10ms
- Memory efficiency: Minimal AST memory footprint
- Code quality: Clean, maintainable parser implementation

### Testing Strategy

#### Official Test Suite Validation
```python
# Test against all 934 FHIRPath R4 official test cases
def test_official_fhirpath_cases():
    for test_case in load_official_tests():
        ast = parser.parse(test_case.expression)
        assert ast is not None
        assert ast.expression.node_type in VALID_NODE_TYPES
```

#### Grammar Coverage Testing
```python
# Ensure complete grammar coverage
def test_grammar_coverage():
    test_cases = [
        "Patient.name.given.first()",
        "Patient.name.where(use = 'official')",
        "Patient.birthDate >= @2000-01-01",
        # ... all grammar constructs
    ]
```

## Impact Analysis

### Benefits
- **Foundation for Compliance**: Enables 100% FHIRPath specification compliance
- **Architecture Clarity**: Clean AST provides clear structure for subsequent layers
- **Performance Potential**: AST designed for population-scale optimization
- **Specification Unification**: Single parsing foundation for all healthcare expression languages

### Risks and Mitigations
- **Grammar Complexity**: FHIRPath grammar is extensive
  - *Mitigation*: Incremental development with continuous testing
- **Performance Concerns**: Complex parsing could impact performance
  - *Mitigation*: Focus on correctness first, optimize based on profiling
- **Scope Creep**: Temptation to include execution logic
  - *Mitigation*: Strict separation of concerns, parsing only

### Success Dependencies
- **Official Test Cases**: Access to complete FHIRPath R4 test suite
- **Grammar Specification**: Clear understanding of FHIRPath R4 grammar rules
- **Development Focus**: Maintaining scope discipline on parsing only

## Alternatives Considered

### Alternative 1: Extend Existing Parser
**Description**: Modify current FHIRPath parsing logic to improve compliance

**Why Rejected**: Current implementation has fundamental architectural issues that prevent reaching 100% compliance

### Alternative 2: Third-Party Parser Integration
**Description**: Integrate existing FHIRPath parser library

**Why Rejected**: Need complete control over AST structure for population-scale optimization; external dependencies limit architectural flexibility

### Alternative 3: Multi-Specification Parser
**Description**: Build combined parser for FHIRPath, SQL-on-FHIR, and CQL simultaneously

**Why Rejected**: Too broad for foundational component; FHIRPath foundation must be solid before building extensions

## Success Metrics

### Primary Success Criteria
- **100% Parse Success**: All 934 FHIRPath R4 official test cases parse successfully
- **AST Completeness**: Generated AST captures complete expression semantics
- **Error Handling Quality**: Clear, actionable error messages for invalid expressions

### Validation Approach
- **Daily**: Automated testing against official test suite
- **Weekly**: Performance benchmarking and memory profiling
- **Milestone**: Grammar coverage verification and AST structure validation

## Next Steps After Approval

1. **Set up parsing framework** with lexer/parser architecture
2. **Implement core grammar** for path navigation and basic functions
3. **Establish testing infrastructure** with official test case integration
4. **Iterate toward 100% compliance** with systematic grammar expansion

---

*This PEP establishes the critical foundation for unified FHIRPath architecture through correct, complete parsing and well-structured AST representation.*