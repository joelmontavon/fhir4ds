# Task SP-001-005: Grammar Completion

**Milestone**: M-2025-Q1-001 - FHIRPath Parser and AST Foundation
**Sprint**: 3
**Status**: In Progress
**Assigned To**: Senior Solution Architect
**Created**: 25-09-2025
**Updated**: 25-09-2025

---

## 1. Description

This task focuses on completing the implementation of the FHIRPath R4 grammar within the parser. It builds upon the foundational parser framework established in `SP-001-003`. The primary goal is to extend the parser's capabilities to handle the full range of FHIRPath expressions, including advanced functions, complex expressions, and polymorphic navigation, as specified in the official FHIRPath R4 standard.

## 2. Objectives

- **Enhance AST**: Introduce new AST nodes to represent more complex FHIRPath constructs like `Indexer` and `InvocationExpression`.
- **Implement Full Grammar**: Extend the parser to recognize and correctly parse all FHIRPath R4 grammar rules.
- **Support Advanced Features**: Ensure the parser can handle:
    - **Indexer**: `Patient.name[0]`
    - **Invocation Expressions**: `Patient.name.given.first()`
    - **Polymorphic Navigation**: `observation.value.ofType(Quantity)`
    - **Complex Boolean Logic**: `(a or b) and c`
    - **Mathematical Expressions**: `(5 + 3) * 2`
- **Maintain Correctness**: Ensure the generated AST accurately reflects the semantics of the parsed expressions.

## 3. Scope

### In Scope
- Modifying `fhir4ds/ast/nodes.py` to add new AST node types.
- Updating the `FHIRPathParser` to implement the complete grammar.
- Adding unit tests to validate the new parsing logic.
- Ensuring all changes are integrated cleanly with the existing lexer and AST structures.

### Out of Scope
- **Execution Logic**: This task is strictly limited to parsing. No expression evaluation or execution will be implemented.
- **Official Test Integration**: While the implementation should be testable against the official suite, the full integration (`SP-001-004`) is a separate task.
- **Performance Optimization**: Major performance enhancements are deferred to `SP-001-007`. The focus here is on correctness.

## 4. Dependencies

- **`SP-001-003` (Parser Framework)**: This task directly depends on the completion of the core parser framework. The framework must be in place before grammar expansion can begin.

## 5. Deliverables

- **Updated `fhir4ds/ast/nodes.py`**: With new `Indexer` and `InvocationExpression` node classes.
- **Updated Parser**: A modified parser that can handle the complete FHIRPath R4 grammar.
- **Unit Tests**: A suite of new unit tests that cover the added grammar rules and AST nodes.
- **Clean Integration**: All new code must be cleanly integrated and pass existing tests.

## 6. Acceptance Criteria

- The parser correctly parses expressions using `Indexer` syntax.
- The parser correctly parses chained function calls (`InvocationExpression`).
- The parser correctly handles operator precedence for mathematical and logical operators.
- All new code is accompanied by corresponding unit tests.
- All existing unit tests continue to pass.
- The code adheres to the project's coding standards.

---