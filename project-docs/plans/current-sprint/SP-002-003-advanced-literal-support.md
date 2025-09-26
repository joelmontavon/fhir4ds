# Task: Advanced Literal Support

**Task ID**: SP-002-003
**Sprint**: SP-002
**Task Name**: Advanced Literal Support (DateTime, Collections, Quantities)
**Assignee**: Junior Developer
**Created**: 26-01-2025
**Last Updated**: 26-01-2025
**Dependencies**: None (can run parallel with SP-002-002)

---

## Task Overview

### Description
Complete the literal support system by implementing missing literal types that are tokenized by the lexer but not fully parsed by the parser. This includes DateTime/Time literals, Collection literals, and enhanced Quantity literal support. This task bridges the gap between lexer capabilities and parser functionality.

### Category
- [x] Feature Implementation
- [ ] Testing Infrastructure
- [ ] Architecture Enhancement
- [ ] Performance Optimization
- [ ] Bug Fix
- [ ] Documentation
- [ ] Process Improvement

### Priority
- [ ] Critical (Blocker for sprint goals)
- [x] High (Important for sprint success)
- [ ] Medium (Valuable but not essential)
- [ ] Low (Stretch goal)

---

## Requirements

### Functional Requirements

#### DateTime and Time Literals
1. **DateTime Literal Parsing**: Support full ISO 8601 datetime formats
   ```
   @2024-01-01T12:30:00Z
   @2024-01-01T12:30:00+05:00
   @2024-01-01T12:30:00.123Z
   ```

2. **Date Literal Parsing**: Support date-only formats
   ```
   @2024-01-01
   @2024-01
   @2024
   ```

3. **Time Literal Parsing**: Support time-only formats
   ```
   @T12:30:00
   @T12:30:00.123
   @T12:30
   ```

#### Collection Literals
4. **Array Collection Literals**: Support explicit collection syntax
   ```
   {1, 2, 3}
   {'a', 'b', 'c'}
   {Patient.name, Patient.id}
   ```

5. **Empty Collection Literals**: Support empty collections
   ```
   {}
   ```

6. **Mixed Type Collections**: Support collections with different types
   ```
   {1, 'text', true}
   ```

#### Enhanced Quantity Literals
7. **Quantity Literal Parsing**: Complete quantity support with units
   ```
   5 'mg'
   10.5 'cm'
   100 'kg'
   ```

8. **UCUM Unit Support**: Support common UCUM units
   ```
   5 'mg'    # milligrams
   10 'cm'   # centimeters
   98.6 'Cel' # degrees Celsius
   ```

### Non-Functional Requirements
- **Performance**: Literal parsing should not significantly impact overall performance
- **Memory Efficiency**: Literal objects should have minimal memory overhead
- **Error Handling**: Clear error messages for malformed literals
- **Specification Compliance**: Full compliance with FHIRPath literal specifications

---

## Technical Specifications

### Enhanced AST Nodes
```python
@dataclass(frozen=True)
class DateTimeLiteral(Literal):
    """DateTime literal like @2024-01-01T12:30:00Z"""
    value: datetime
    precision: DateTimePrecision  # year, month, day, hour, minute, second, millisecond
    timezone: Optional[timezone]

    def accept(self, visitor: "ASTVisitor[T]") -> T:
        return visitor.visit_datetime_literal(self)

@dataclass(frozen=True)
class TimeLiteral(Literal):
    """Time literal like @T12:30:00"""
    value: time
    precision: TimePrecision  # hour, minute, second, millisecond

    def accept(self, visitor: "ASTVisitor[T]") -> T:
        return visitor.visit_time_literal(self)

@dataclass(frozen=True)
class CollectionLiteral(Literal):
    """Collection literal like {1, 2, 3}"""
    elements: List[FHIRPathNode]

    @property
    def children(self) -> List[FHIRPathNode]:
        return self.elements

    def accept(self, visitor: "ASTVisitor[T]") -> T:
        return visitor.visit_collection_literal(self)

@dataclass(frozen=True)
class QuantityLiteral(Literal):
    """Quantity literal like 5 'mg'"""
    value: Decimal
    unit: str

    def accept(self, visitor: "ASTVisitor[T]") -> T:
        return visitor.visit_quantity_literal(self)
```

### DateTime Parsing Support
```python
from datetime import datetime, time, timezone
from decimal import Decimal
import re

class DateTimeParser:
    """Parser for FHIRPath DateTime/Time literals"""

    # ISO 8601 datetime pattern
    DATETIME_PATTERN = re.compile(
        r'@(?P<year>\d{4})'
        r'(?:-(?P<month>\d{2}))?'
        r'(?:-(?P<day>\d{2}))?'
        r'(?:T(?P<hour>\d{2})'
        r'(?::(?P<minute>\d{2}))?'
        r'(?::(?P<second>\d{2}))?'
        r'(?:\.(?P<millisecond>\d{1,3}))?'
        r'(?P<timezone>Z|[+-]\d{2}:\d{2})?)?'
    )

    # Time-only pattern
    TIME_PATTERN = re.compile(
        r'@T(?P<hour>\d{2})'
        r'(?::(?P<minute>\d{2}))?'
        r'(?::(?P<second>\d{2}))?'
        r'(?:\.(?P<millisecond>\d{1,3}))?'
    )

    def parse_datetime(self, literal: str) -> DateTimeLiteral:
        """Parse datetime literal into DateTimeLiteral AST node"""

    def parse_time(self, literal: str) -> TimeLiteral:
        """Parse time literal into TimeLiteral AST node"""
```

### Collection Parsing Support
```python
class CollectionParser:
    """Parser for FHIRPath Collection literals"""

    def parse_collection(self, elements: List[FHIRPathNode]) -> CollectionLiteral:
        """Parse collection elements into CollectionLiteral AST node"""
        return CollectionLiteral(
            elements=elements,
            source_location=self._get_source_location(),
            metadata=self._infer_collection_metadata(elements)
        )

    def _infer_collection_metadata(self, elements: List[FHIRPathNode]) -> PopulationMetadata:
        """Infer metadata for collection based on elements"""
        return PopulationMetadata(
            cardinality=Cardinality.COLLECTION,
            fhir_type="Collection",
            complexity_score=len(elements),
            dependencies=set()
        )
```

### Enhanced Parser Integration
```python
# Enhanced _parse_primary method in Parser class
def _parse_primary(self) -> FHIRPathNode:
    """Parse primary expressions including enhanced literals"""

    # Existing literal support...

    if self._match(TokenType.DATETIME_LITERAL):
        return self._parse_datetime_literal()

    if self._match(TokenType.TIME_LITERAL):
        return self._parse_time_literal()

    if self._match(TokenType.QUANTITY_LITERAL):
        return self._parse_quantity_literal()

    if self._match(TokenType.LBRACE):
        return self._parse_collection_literal()

def _parse_datetime_literal(self) -> DateTimeLiteral:
    """Parse datetime literal token into AST node"""
    token = self._previous()
    parser = DateTimeParser()
    return parser.parse_datetime(token.value)

def _parse_collection_literal(self) -> CollectionLiteral:
    """Parse collection literal {element1, element2, ...}"""
    elements = []

    if not self._check(TokenType.RBRACE):
        elements.append(self._parse_expression())
        while self._match(TokenType.COMMA):
            elements.append(self._parse_expression())

    self._consume(TokenType.RBRACE, "Expect '}' after collection elements.")

    return CollectionLiteral(
        elements=elements,
        source_location=self._get_source_location(),
        metadata=self._infer_collection_metadata(elements)
    )
```

---

## Implementation Plan

### Day 1: DateTime and Time Literal Support
- **Hour 1-4**: Implement DateTimeParser with ISO 8601 support
- **Hour 5-8**: Create DateTimeLiteral and TimeLiteral AST nodes
- **Hour 9-12**: Integrate datetime parsing with parser
- **Hour 13-16**: Test datetime literals against lexer tokens and official test cases

### Day 2: Collection Literal Support
- **Hour 1-4**: Implement CollectionLiteral AST node and parsing logic
- **Hour 5-8**: Add collection literal parsing to parser (_parse_collection_literal)
- **Hour 9-12**: Handle empty collections and mixed types
- **Hour 13-16**: Test collection literals with various element types

### Day 3: Quantity Literals and Integration
- **Hour 1-4**: Complete QuantityLiteral parsing with UCUM unit support
- **Hour 5-8**: Integrate all new literal types with existing parser
- **Hour 9-12**: Update visitor pattern to handle new literal types
- **Hour 13-16**: Comprehensive testing and validation against official test cases

---

## Acceptance Criteria

### Primary Acceptance Criteria
1. **✅ DateTime/Time Literal Support**:
   - All ISO 8601 datetime formats parse correctly
   - Date-only and time-only literals supported
   - Proper timezone and precision handling

2. **✅ Collection Literal Support**:
   - Collection syntax `{element1, element2}` parses correctly
   - Empty collections `{}` supported
   - Mixed type collections functional
   - Collection operations work with literal collections

3. **✅ Enhanced Quantity Support**:
   - Quantity literals with units parse correctly
   - Common UCUM units supported
   - Decimal precision maintained

4. **✅ Parser Integration**:
   - All new literals integrate seamlessly with existing parser
   - No regression in existing literal parsing
   - New literals work in complex expressions

### Quality Gates
- **No parsing regression**: All previously working literals continue to work
- **Specification compliance**: New literals match FHIRPath specification behavior
- **Error handling**: Malformed literals produce clear error messages
- **Performance**: Literal parsing performance remains acceptable

---

## Testing Strategy

### Unit Testing
1. **DateTime/Time Literal Testing**:
   ```python
   test_cases = [
       "@2024-01-01T12:30:00Z",
       "@2024-01-01T12:30:00+05:00",
       "@2024-01-01",
       "@T12:30:00",
       "@T12:30:00.123"
   ]
   ```

2. **Collection Literal Testing**:
   ```python
   test_cases = [
       "{1, 2, 3}",
       "{'a', 'b', 'c'}",
       "{true, false}",
       "{}",  # empty collection
       "{1, 'text', true}"  # mixed types
   ]
   ```

3. **Quantity Literal Testing**:
   ```python
   test_cases = [
       "5 'mg'",
       "10.5 'cm'",
       "100 'kg'",
       "98.6 'Cel'"
   ]
   ```

### Integration Testing
- **Complex Expression Testing**: Test literals in complex expressions
- **Function Integration**: Test literals with function calls
- **Official Test Cases**: Validate against relevant official test cases

---

## Deliverables

### Code Deliverables
1. **Enhanced AST Nodes**: New literal node types in `fhir4ds/ast/nodes.py`
2. **DateTime Parser**: `fhir4ds/parser/literals/datetime_parser.py`
3. **Collection Parser**: Enhanced collection parsing in `fhir4ds/parser/parser.py`
4. **Quantity Parser**: Enhanced quantity parsing in `fhir4ds/parser/parser.py`
5. **Visitor Updates**: Enhanced visitor pattern in `fhir4ds/ast/visitors.py`

### Documentation Deliverables
1. **Literal Support Documentation**: Complete guide to supported literal types
2. **Usage Examples**: Examples of all new literal types in expressions
3. **Error Handling Guide**: Common errors and solutions for literal parsing

### Testing Deliverables
1. **Literal Test Suite**: Comprehensive tests for all literal types
2. **Integration Tests**: Tests for literals in complex expressions
3. **Official Test Results**: Updated compliance with literal-related test cases

---

## Success Metrics

### Quantitative Metrics
- **Literal Type Coverage**: 100% of identified literal types implemented
- **Test Case Improvement**: Measurable improvement in literal-related official test cases
- **Performance Impact**: <10% parsing performance impact for literal parsing
- **Error Handling**: 100% of malformed literals produce clear error messages

### Qualitative Metrics
- **Specification Compliance**: All literals behave according to FHIRPath specification
- **Developer Experience**: Literals are intuitive to use and debug
- **Integration Quality**: Literals work seamlessly in all expression contexts

---

## Dependencies and Blockers

### Dependencies
1. **Lexer Token Support**: Requires existing lexer support for literal tokens
2. **AST Framework**: Requires completed AST node framework from SP-001
3. **Parser Framework**: Requires basic parser structure for integration

### Potential Blockers
1. **DateTime Complexity**: ISO 8601 datetime parsing may be more complex than expected
2. **Collection Type Inference**: Determining collection types may require advanced logic
3. **UCUM Unit Support**: Quantity unit validation may require external libraries

---

## Risk Mitigation

### Technical Risks
1. **DateTime Parsing Complexity**:
   - **Mitigation**: Use Python's built-in datetime parsing where possible
   - **Contingency**: Implement simplified datetime support if full ISO 8601 is too complex

2. **Collection Type Safety**:
   - **Mitigation**: Start with simple collections, add type validation incrementally
   - **Contingency**: Allow mixed-type collections initially, add constraints later

3. **Performance Impact**:
   - **Mitigation**: Profile literal parsing performance during development
   - **Contingency**: Optimize parsing algorithms if performance degrades

---

**Task completes the literal foundation by bridging lexer capabilities with parser functionality, enabling full FHIRPath literal support.**