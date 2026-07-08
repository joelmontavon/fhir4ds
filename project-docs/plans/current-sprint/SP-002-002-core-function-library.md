# Task: Core FHIRPath Function Library Implementation

**Task ID**: SP-002-002
**Sprint**: SP-002
**Task Name**: Core FHIRPath Function Library Implementation
**Assignee**: Senior Architect + Junior Developer (testing)
**Created**: 26-01-2025
**Last Updated**: 26-01-2025
**Dependencies**: SP-002-001 (Official Test Suite Integration)

---

## Task Overview

### Description
Implement the essential FHIRPath function library to enable realistic healthcare expressions and significantly improve specification compliance. Focus on core functions that appear frequently in official test cases and real-world FHIRPath expressions. This task transforms the parser from basic path navigation to a functional FHIRPath expression engine.

### Category
- [x] Feature Implementation
- [ ] Testing Infrastructure
- [ ] Architecture Enhancement
- [ ] Performance Optimization
- [ ] Bug Fix
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

#### Core Collection Functions
1. **where()**: Filter collections based on boolean expressions
   ```
   Patient.name.where(use = 'official')
   Patient.telecom.where(system = 'phone')
   ```

2. **select()**: Transform collections by extracting/computing values
   ```
   Patient.name.select(given + ' ' + family)
   Patient.telecom.select(value)
   ```

3. **first()**: Get first element of collection
   ```
   Patient.name.first()
   Patient.name.where(use = 'official').first()
   ```

4. **last()**: Get last element of collection
   ```
   Patient.name.last()
   ```

5. **tail()**: Get all elements except first
   ```
   Patient.name.tail()
   ```

#### Boolean Functions
6. **exists()**: Check if collection is non-empty
   ```
   Patient.name.where(use = 'official').exists()
   Patient.telecom.where(system = 'email').exists()
   ```

7. **empty()**: Check if collection is empty
   ```
   Patient.name.where(use = 'temp').empty()
   ```

8. **not()**: Boolean negation
   ```
   Patient.active.not()
   ```

#### Aggregate Functions
9. **count()**: Count elements in collection
   ```
   Patient.name.count()
   Patient.telecom.where(system = 'phone').count()
   ```

10. **sum()**: Sum numeric values
    ```
    Observation.component.value.sum()
    ```

11. **avg()**: Average of numeric values
    ```
    Observation.component.value.avg()
    ```

### Non-Functional Requirements
- **Performance**: Function calls should not significantly impact parsing performance
- **Memory Efficiency**: Function implementations should not leak memory
- **Error Handling**: Clear error messages for invalid function usage
- **Extensibility**: Function registry system to enable easy addition of new functions

---

## Technical Specifications

### Function Registry Architecture
```python
class FHIRPathFunctionRegistry:
    """Registry for FHIRPath function implementations"""

    def __init__(self):
        self._functions: Dict[str, FHIRPathFunction] = {}

    def register_function(self, name: str, function: FHIRPathFunction):
        """Register a new FHIRPath function"""
        self._functions[name] = function

    def get_function(self, name: str) -> Optional[FHIRPathFunction]:
        """Get function implementation by name"""
        return self._functions.get(name)

    def list_functions(self) -> List[str]:
        """List all registered function names"""
        return list(self._functions.keys())
```

### Function Implementation Interface
```python
from abc import ABC, abstractmethod
from typing import Any, List, Optional

class FHIRPathFunction(ABC):
    """Base class for FHIRPath function implementations"""

    @abstractmethod
    def name(self) -> str:
        """Function name as it appears in FHIRPath expressions"""

    @abstractmethod
    def validate_arguments(self, arguments: List[FHIRPathNode]) -> List[ValidationError]:
        """Validate function arguments"""

    @abstractmethod
    def create_ast_node(self, expression: FHIRPathNode, arguments: List[FHIRPathNode]) -> FunctionCall:
        """Create AST node for this function call"""
```

### Core Function Implementations
```python
class WhereFunction(FHIRPathFunction):
    """Implementation of where() function"""

    def name(self) -> str:
        return "where"

    def validate_arguments(self, arguments: List[FHIRPathNode]) -> List[ValidationError]:
        if len(arguments) != 1:
            return [ValidationError("where() requires exactly one argument")]
        return []

    def create_ast_node(self, expression: FHIRPathNode, arguments: List[FHIRPathNode]) -> FunctionCall:
        return FunctionCall(
            name=Identifier(value="where", ...),
            expression=expression,
            arguments=arguments,
            source_location=...,
            metadata=...
        )

class FirstFunction(FHIRPathFunction):
    """Implementation of first() function"""

    def name(self) -> str:
        return "first"

    def validate_arguments(self, arguments: List[FHIRPathNode]) -> List[ValidationError]:
        if len(arguments) != 0:
            return [ValidationError("first() takes no arguments")]
        return []
```

### Parser Integration
```python
# Enhanced parser method for function calls
def _parse_function_call(self, expression: FHIRPathNode, function_name: str) -> FHIRPathNode:
    """Parse function call using function registry"""

    # Get function implementation from registry
    function_impl = self.function_registry.get_function(function_name)
    if not function_impl:
        raise ParseError(f"Unknown function: {function_name}")

    # Parse arguments
    arguments = []
    if not self._check(TokenType.RPAREN):
        arguments.append(self._parse_expression())
        while self._match(TokenType.COMMA):
            arguments.append(self._parse_expression())

    # Validate arguments
    validation_errors = function_impl.validate_arguments(arguments)
    if validation_errors:
        raise ParseError(f"Invalid arguments for {function_name}: {validation_errors[0].message}")

    # Create AST node
    return function_impl.create_ast_node(expression, arguments)
```

---

## Implementation Plan

### Day 1-2: Function Registry and Framework
- **Hour 1-4**: Implement function registry system and base function interface
- **Hour 5-8**: Create function validation framework
- **Hour 9-12**: Integrate function registry with parser
- **Hour 13-16**: Test framework with simple functions (first, last)

### Day 3-4: Core Collection Functions
- **Hour 1-4**: Implement where() function with boolean expression support
- **Hour 5-8**: Implement select() function with transformation expressions
- **Hour 9-12**: Implement first(), last(), tail() functions
- **Hour 13-16**: Test collection functions against official test cases

### Day 5-6: Boolean and Aggregate Functions
- **Hour 1-4**: Implement exists(), empty(), not() functions
- **Hour 5-8**: Implement count() function
- **Hour 9-12**: Implement sum(), avg() aggregate functions
- **Hour 13-16**: Test all functions against official test cases

### Day 7: Integration and Validation
- **Hour 1-4**: Complete function integration with parser
- **Hour 5-8**: Run complete official test suite with new functions
- **Hour 9-12**: Fix any integration issues and test failures
- **Hour 13-16**: Performance testing and optimization

---

## Acceptance Criteria

### Primary Acceptance Criteria
1. **✅ Complete Function Registry**:
   - Function registry system operational
   - All 11 core functions registered and functional
   - Easy extension mechanism for future functions

2. **✅ Function Implementations**:
   - All core functions parse correctly in expressions
   - Functions handle argument validation properly
   - Clear error messages for invalid function usage

3. **✅ Parser Integration**:
   - Function calls integrated seamlessly with existing parser
   - No regression in existing parsing functionality
   - Function calls create proper AST nodes

4. **✅ Test Validation**:
   - All implemented functions pass relevant official test cases
   - Function calls work correctly in complex expressions
   - Performance impact within acceptable bounds

### Quality Gates
- **No parsing regression**: All previously working expressions continue to work
- **Function accuracy**: Each function behaves according to FHIRPath specification
- **Error handling**: Invalid function calls produce clear, helpful error messages
- **Performance**: Function implementation doesn't significantly impact parsing speed

---

## Function Implementation Details

### High Priority Functions (Day 3-4)

#### where() Function
```python
# Expected usage and behavior
Patient.name.where(use = 'official')  # Filter names by use
Patient.telecom.where(system = 'phone')  # Filter telecom by system

# Implementation requirements:
# - Accept one boolean expression argument
# - Parse complex boolean expressions (comparisons, logical operators)
# - Create appropriate AST representation
```

#### select() Function
```python
# Expected usage and behavior
Patient.name.select(given)  # Extract given names
Patient.telecom.select(value)  # Extract telecom values

# Implementation requirements:
# - Accept one transformation expression argument
# - Support property access and simple transformations
# - Handle collection results properly
```

#### first(), last(), tail() Functions
```python
# Expected usage and behavior
Patient.name.first()  # Get first name
Patient.name.last()   # Get last name
Patient.name.tail()   # Get all names except first

# Implementation requirements:
# - No arguments for first() and last()
# - Proper handling of empty collections
# - Clear semantics for single-element collections
```

### Medium Priority Functions (Day 5-6)

#### Boolean Functions
```python
# exists() - check if collection is non-empty
Patient.name.where(use = 'official').exists()

# empty() - check if collection is empty
Patient.name.where(use = 'temp').empty()

# not() - boolean negation
Patient.active.not()
```

#### Aggregate Functions
```python
# count() - count collection elements
Patient.name.count()
Patient.telecom.count()

# sum(), avg() - numeric aggregation
Observation.component.value.sum()
Observation.component.value.avg()
```

---

## Testing Strategy

### Unit Testing
1. **Individual Function Testing**:
   - Test each function in isolation with simple expressions
   - Validate argument parsing and validation
   - Test error cases and edge conditions

2. **Integration Testing**:
   - Test functions in complex expressions
   - Test function chaining: `Patient.name.where(use='official').first()`
   - Test functions with various data types

3. **Official Test Case Validation**:
   - Run relevant official test cases for each implemented function
   - Validate that function implementations match specification behavior
   - Ensure no regression in overall test suite compliance

### Performance Testing
- **Function Call Overhead**: Measure parsing performance impact of function calls
- **Complex Expression Performance**: Test expressions with multiple chained functions
- **Memory Usage**: Ensure function implementations don't leak memory

---

## Deliverables

### Code Deliverables
1. **Function Registry System**: `fhir4ds/parser/functions/registry.py`
2. **Base Function Interface**: `fhir4ds/parser/functions/base.py`
3. **Core Function Implementations**: `fhir4ds/parser/functions/core/`
4. **Parser Integration**: Enhanced `fhir4ds/parser/parser.py`
5. **AST Node Updates**: Enhanced `fhir4ds/ast/nodes.py` for function calls

### Documentation Deliverables
1. **Function Library Documentation**: API docs for each implemented function
2. **Usage Examples**: Comprehensive examples of function usage
3. **Extension Guide**: How to add new functions to the registry
4. **Performance Impact**: Documented performance characteristics

### Testing Deliverables
1. **Function Test Suite**: Comprehensive tests for all implemented functions
2. **Integration Tests**: Tests for function combinations and chaining
3. **Official Test Results**: Updated compliance report with function implementations

---

## Success Metrics

### Quantitative Metrics
- **Function Coverage**: 11/11 core functions implemented and operational
- **Test Suite Improvement**: Measurable increase in official test suite pass rate
- **Performance Impact**: <20% parsing performance degradation for function calls
- **Error Handling**: 100% of invalid function calls produce clear error messages

### Qualitative Metrics
- **Function Accuracy**: All functions behave according to FHIRPath specification
- **Developer Experience**: Functions easy to use and debug
- **Extension Simplicity**: New functions can be added easily
- **Integration Quality**: Functions integrate seamlessly with existing parser

---

## Dependencies and Blockers

### Dependencies
1. **SP-002-001**: Official test suite integration (to validate function implementations)
2. **SP-001 Parser Foundation**: Requires completed basic parser and AST system
3. **Function Registry Design**: Requires architectural decision on function system

### Potential Blockers
1. **Complex Function Semantics**: Some functions may have complex specification requirements
2. **Parser Architecture Limitations**: Current parser may need modification for function support
3. **Performance Requirements**: Function implementations may impact parsing performance

---

## Risk Mitigation

### Technical Risks
1. **Function Complexity Underestimation**:
   - **Mitigation**: Implement simplest functions first, defer complex ones if needed
   - **Contingency**: Reduce scope to essential functions (where, first, exists, count)

2. **Parser Integration Issues**:
   - **Mitigation**: Start with simple function integration, expand gradually
   - **Contingency**: Create separate function parsing layer if needed

3. **Performance Regression**:
   - **Mitigation**: Continuous performance monitoring during implementation
   - **Contingency**: Optimize function call mechanism if performance degrades

### Scope Risks
1. **Feature Creep**:
   - **Mitigation**: Strict adherence to 11 core functions list
   - **Contingency**: Defer advanced functions to next sprint if timeline at risk

---

**Task transforms parser from basic navigation to functional FHIRPath expression engine with essential healthcare query capabilities.**