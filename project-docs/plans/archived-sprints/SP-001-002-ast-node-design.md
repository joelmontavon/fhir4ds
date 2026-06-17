# Task: AST Node Structure Design

**Task ID**: SP-001-002
**Sprint**: Sprint 1
**Task Name**: Design and Implement AST Node Structure for FHIRPath
**Assignee**: Junior Developer B
**Created**: 25-01-2025
**Last Updated**: 25-01-2025

---

## Task Overview

### Description
Design and implement the complete Abstract Syntax Tree (AST) node structure for FHIRPath expressions. This includes defining all node types needed to represent FHIRPath grammar constructs, implementing the node hierarchy, and creating validation mechanisms. The AST must be designed for population-scale optimization and future CTE generation while maintaining clean separation from execution logic.

### Category
- [x] Architecture Enhancement
- [ ] Feature Implementation
- [ ] Bug Fix
- [ ] Performance Optimization
- [ ] Testing
- [ ] Documentation
- [ ] Process Improvement

### Priority
- [x] Critical (Blocker for sprint goals)
- [ ] High (Important for sprint success)
- [ ] Medium (Valuable but not essential)
- [ ] Low (Stretch goal)

---

## Requirements

### Functional Requirements
1. **Complete Node Type Coverage**: AST nodes for all FHIRPath constructs:
   - Path expressions: `Patient.name.given`
   - Function calls: `first()`, `where(condition)`, `select(expression)`
   - Binary operations: `and`, `or`, `=`, `!=`, `<`, `>`, `+`, `-`, `*`, `/`
   - Unary operations: `not`, `-` (negation)
   - Literals: strings, numbers, booleans, dates, quantities
   - Collections: arrays, sets, and ranges
   - Polymorphic navigation: `ofType()`, `as()`

2. **Population-Scale Metadata**: Each node includes metadata for optimization:
   - Cardinality hints (single vs. collection)
   - Type information for optimization
   - Dependency tracking for CTE generation
   - Complexity metrics for query planning

3. **Immutable Design**: AST nodes are immutable after creation for thread safety
4. **Visitor Pattern Support**: Structure supports visitor pattern for analysis and transformation
5. **Source Location Preservation**: All nodes track original source location for debugging

### Non-Functional Requirements
- **Memory Efficiency**: Minimal memory footprint per node
- **Type Safety**: Complete type hints and validation
- **Extensibility**: Easy addition of new node types for future specifications
- **Serialization**: AST can be serialized/deserialized for caching

### Acceptance Criteria
- [ ] All FHIRPath grammar constructs have corresponding AST nodes
- [ ] AST structure accurately represents expression semantics and precedence
- [ ] Population-scale metadata is comprehensive and useful for optimization
- [ ] Visitor pattern implementation supports traversal and transformation
- [ ] Source location information is preserved throughout AST
- [ ] AST validation catches semantic errors not detectable during parsing
- [ ] Memory usage is efficient with minimal node overhead
- [ ] Complete type hints and comprehensive unit tests

---

## Technical Specifications

### Affected Components
- **fhir4ds/ast/nodes.py**: Core AST node definitions (new)
- **fhir4ds/ast/visitors.py**: Visitor pattern implementation (new)
- **fhir4ds/ast/validation.py**: AST validation framework (new)
- **fhir4ds/ast/metadata.py**: Population-scale metadata definitions (new)

### File Modifications
- **fhir4ds/ast/__init__.py**: AST module initialization
- **fhir4ds/ast/nodes.py**: All AST node class definitions
- **fhir4ds/ast/visitors.py**: Base visitor and common visitors
- **fhir4ds/ast/validation.py**: Semantic validation framework
- **fhir4ds/ast/metadata.py**: Optimization metadata structures
- **tests/unit/ast/test_nodes.py**: Comprehensive AST node tests
- **tests/unit/ast/test_visitors.py**: Visitor pattern tests
- **tests/unit/ast/test_validation.py**: Validation framework tests

### Database Considerations
- **No database dependencies**: Pure AST structure with no database interaction
- **Future CTE Integration**: AST design must support SQL CTE generation patterns

---

## Dependencies

### Prerequisites
1. **Token Definitions**: Token types from lexer implementation (can work in parallel)
2. **Project Structure**: Basic fhir4ds module structure
3. **Architecture Principles**: Understanding of population-scale optimization requirements

### Blocking Tasks
- None (can work in parallel with lexer implementation)

### Dependent Tasks
- **SP-001-003**: Parser Framework Implementation (uses AST nodes)
- **SP-002-001**: AST Validation Implementation (uses node structure)

---

## Implementation Approach

### High-Level Strategy
Design a hierarchical AST node structure using Python dataclasses for immutability and type safety. Implement visitor pattern for extensible AST operations. Include population-scale metadata from the start to support future CTE generation without architectural changes.

### Implementation Steps

1. **Base Node Architecture** (6 hours)
   - Estimated Time: 6 hours
   - Key Activities:
     - Create base FHIRPathNode abstract class with common properties
     - Implement SourceLocation and NodeMetadata structures
     - Define visitor pattern interfaces and base visitor
     - Create node type enumeration and hierarchy
   - Validation: Clean base architecture with proper inheritance

2. **Expression Node Types** (10 hours)
   - Estimated Time: 10 hours
   - Key Activities:
     - PathExpression: `Patient.name.given` navigation chains
     - FunctionCall: `first()`, `where()`, `select()` with arguments
     - BinaryOperation: `and`, `or`, comparisons, arithmetic
     - UnaryOperation: `not`, negation operations
     - ConditionalExpression: ternary operations if supported
   - Validation: All expression types correctly represent FHIRPath semantics

3. **Literal and Data Node Types** (6 hours)
   - Estimated Time: 6 hours
   - Key Activities:
     - StringLiteral, NumberLiteral, BooleanLiteral nodes
     - DateLiteral, TimeLiteral, DateTimeLiteral for temporal values
     - QuantityLiteral for FHIR quantity values with units
     - CollectionLiteral for arrays and sets
     - Identifier nodes for property and function names
   - Validation: All FHIRPath literal types supported with proper validation

4. **Advanced Construct Nodes** (8 hours)
   - Estimated Time: 8 hours
   - Key Activities:
     - PolymorphicNavigation: `ofType()`, `as()` type operations
     - AggregationFunction: `count()`, `sum()`, `avg()` operations
     - FilterExpression: optimized `where()` clause representation
     - IndexExpression: array indexing with `[n]` syntax
   - Validation: Complex FHIRPath constructs properly represented

5. **Population-Scale Metadata** (6 hours)
   - Estimated Time: 6 hours
   - Key Activities:
     - Cardinality metadata (single/collection/optional)
     - Type information for FHIR resource optimization
     - Dependency tracking for CTE ordering
     - Complexity metrics for query planning
     - Resource impact analysis metadata
   - Validation: Metadata supports population-scale CTE generation

6. **Visitor Pattern and Utilities** (6 hours)
   - Estimated Time: 6 hours
   - Key Activities:
     - Implement visitor pattern with double dispatch
     - Create common visitors: AST printer, type checker, complexity analyzer
     - Build AST transformation utilities
     - Implement AST serialization/deserialization
   - Validation: Visitor pattern works correctly with all node types

7. **Testing and Validation** (8 hours)
   - Estimated Time: 8 hours
   - Key Activities:
     - Comprehensive unit tests for all node types
     - Visitor pattern testing with example operations
     - Metadata validation and population-scale readiness testing
     - Performance testing for large AST structures
   - Validation: >95% code coverage and all functionality verified

### Alternative Approaches Considered
- **Simple Dictionary Structure**: Too flexible, lacks type safety
- **Generated AST from Grammar**: Adds build complexity, less control over optimization metadata

---

## Useful Existing Code References

### From Archived Implementation

#### AST Concepts (`archive/fhir4ds/fhirpath/core/generator.py`)
**Lines 125-200**: Node type definitions and metadata
```python
# Study these patterns (adapt for immutable design):
@dataclass
class PathNode:
    path_steps: List[str]
    cardinality: str
    resource_type: Optional[str]
```
**What to reuse**: Cardinality and type tracking concepts
**What to improve**: Immutable design, comprehensive metadata

#### Population Optimization (`archive/fhir4ds/view_runner.py`)
**Lines 89-150**: Population-scale query patterns
```python
# Study successful population patterns:
def generate_population_query(view_def):
    # Patterns for batch processing optimization
    # CTE generation strategies
```
**What to reuse**: Population-first thinking and optimization patterns
**What to improve**: Clean AST integration for CTE generation

#### Type System (`archive/fhir4ds/fhir/type_registry.py`)
**Lines 45-120**: FHIR type definitions and validation
```python
# Reusable type system concepts:
FHIR_PRIMITIVE_TYPES = {
    'string': str,
    'integer': int,
    'boolean': bool,
    # ...
}
```
**What to reuse**: FHIR type definitions and hierarchies
**What to improve**: Integration with AST nodes for type safety

### New Architecture Principles to Follow
1. **Immutable by Default**: Use frozen dataclasses for all AST nodes
2. **Population-First Metadata**: Every node includes optimization hints
3. **Visitor Pattern**: Enable extensible AST operations without node modification
4. **Type Safety**: Complete type hints and runtime validation where beneficial
5. **Future-Ready**: Design accommodates SQL-on-FHIR and CQL extensions

---

## Testing Strategy

### Unit Testing
- **New Tests Required**:
  - Each AST node type with construction and property validation
  - Visitor pattern functionality with all node types
  - Population-scale metadata accuracy and completeness
  - AST serialization/deserialization round trips
  - Memory efficiency testing with large AST structures
- **Coverage Target**: >95% code coverage

### Integration Testing
- **Parser Integration**: Verify AST nodes work correctly with parser output
- **Complex Expression Testing**: Build and validate AST for complex FHIRPath expressions
- **Population Readiness**: Confirm AST supports population-scale optimization patterns

### Performance Testing
- **Memory Usage**: AST nodes should have minimal memory overhead
- **Visitor Performance**: Visitor pattern should efficiently traverse large ASTs
- **Construction Cost**: Node creation should be fast for parser integration

### Manual Testing
- **AST Visualization**: Test AST printer visitor with various expression types
- **Type Validation**: Verify type metadata accuracy for different node types
- **Complexity Analysis**: Validate complexity metrics for optimization planning

---

## Risk Assessment

### Technical Risks
| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| AST structure too complex for simple expressions | Low | Medium | Start simple, add complexity incrementally |
| Population metadata overhead affects performance | Medium | Medium | Profile early, optimize metadata storage |
| Visitor pattern implementation complexity | Low | High | Use established patterns, test thoroughly |

### Implementation Challenges
1. **Metadata Design**: Balancing completeness with performance and complexity
2. **Type System Integration**: Proper FHIR type representation in AST nodes
3. **Future Extensibility**: Designing for SQL-on-FHIR and CQL without over-engineering

### Contingency Plans
- **If AST becomes too complex**: Simplify metadata, focus on correctness first
- **If performance is poor**: Implement lazy metadata evaluation
- **If visitor pattern is problematic**: Use simple method dispatch instead

---

## AST Design Specifications

### Node Hierarchy
```python
FHIRPathNode (abstract base)
├── Expression
│   ├── PathExpression
│   ├── FunctionCall
│   ├── BinaryOperation
│   ├── UnaryOperation
│   └── ConditionalExpression
├── Literal
│   ├── StringLiteral
│   ├── NumberLiteral
│   ├── BooleanLiteral
│   ├── DateLiteral
│   ├── QuantityLiteral
│   └── CollectionLiteral
├── Navigation
│   ├── PropertyAccess
│   ├── IndexAccess
│   ├── PolymorphicNavigation
│   └── FilterExpression
└── Identifier
```

### Population-Scale Metadata Structure
```python
@dataclass(frozen=True)
class PopulationMetadata:
    cardinality: Cardinality  # SINGLE, COLLECTION, OPTIONAL
    fhir_type: Optional[str]  # Patient, Observation, etc.
    complexity_score: int     # For query planning
    dependencies: Set[str]    # For CTE ordering
    resource_impact: ResourceImpact  # Memory/performance hints
```

### Visitor Pattern Interface
```python
class ASTVisitor[T]:
    def visit(self, node: FHIRPathNode) -> T:
        """Double dispatch to appropriate visit method."""

    def visit_path_expression(self, node: PathExpression) -> T:
        """Handle path navigation expressions."""

    def visit_function_call(self, node: FunctionCall) -> T:
        """Handle function call expressions."""

    # ... visit methods for all node types
```

---

## Success Metrics

### Quantitative Measures
- **Node Type Coverage**: 100% of FHIRPath grammar constructs supported
- **Memory Efficiency**: <100 bytes per simple AST node
- **Type Safety**: 100% type hint coverage with mypy validation
- **Test Coverage**: >95% code coverage

### Qualitative Measures
- **Architecture Quality**: Clean hierarchy with minimal coupling
- **Population Readiness**: AST clearly supports population-scale optimization
- **Extensibility**: Easy addition of new node types for future specifications
- **Developer Experience**: Clear APIs and comprehensive documentation

### Compliance Impact
- **FHIRPath Foundation**: AST accurately represents all FHIRPath semantics
- **Future Specifications**: Structure supports SQL-on-FHIR and CQL translation
- **Optimization Ready**: Metadata enables population-scale CTE generation

---

## Documentation Requirements

### Code Documentation
- [x] Complete docstrings for all AST node classes
- [x] Visitor pattern usage examples and best practices
- [x] Population metadata documentation with optimization implications
- [x] Type system integration documentation

### Architecture Documentation
- [ ] AST node hierarchy diagram and relationships
- [ ] Population-scale metadata design rationale
- [ ] Visitor pattern implementation and extension guide
- [ ] Future extensibility considerations for other specifications

---

## Progress Tracking

### Status
- [x] Not Started
- [ ] In Analysis
- [ ] In Development
- [ ] In Testing
- [ ] In Review
- [ ] Completed
- [ ] Blocked

### Completion Checklist
- [ ] All FHIRPath grammar constructs have corresponding AST nodes
- [ ] Population-scale metadata implemented and tested
- [ ] Visitor pattern working with all node types
- [ ] Immutable design verified and tested
- [ ] Source location preservation implemented
- [ ] Memory efficiency validated
- [ ] Unit tests with >95% coverage completed
- [ ] Integration ready for parser implementation
- [ ] Architecture documentation complete

---

**Task Created**: 25-01-2025
**Status**: Not Started

---

*This task establishes the AST foundation enabling accurate FHIRPath representation and future population-scale optimization through CTE generation.*