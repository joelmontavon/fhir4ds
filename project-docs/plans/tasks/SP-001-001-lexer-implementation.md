# Task: FHIRPath Lexer Implementation

**Task ID**: SP-001-001
**Sprint**: Sprint 1
**Task Name**: Complete FHIRPath Lexer and Tokenization System
**Assignee**: Junior Developer A
**Created**: 25-01-2025
**Last Updated**: 25-01-2025

---

## Task Overview

### Description
Implement a comprehensive lexer (tokenizer) for FHIRPath R4 expressions. The lexer must recognize all FHIRPath tokens including keywords, operators, literals, identifiers, and special symbols. This is the foundational component that converts FHIRPath expression strings into structured token streams for the parser.

### Category
- [x] Feature Implementation
- [ ] Bug Fix
- [ ] Architecture Enhancement
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
1. **Complete Token Recognition**: Recognize all FHIRPath R4 grammar tokens including:
   - Keywords: `and`, `or`, `not`, `is`, `as`, `in`, `contains`
   - Operators: `=`, `!=`, `<`, `>`, `<=`, `>=`, `+`, `-`, `*`, `/`, `mod`
   - Delimiters: `.`, `(`, `)`, `[`, `]`, `,`, `|`
   - Literals: strings (`'text'`), numbers (`123`, `45.67`), booleans (`true`, `false`)
   - Identifiers: property names, function names
   - Date/time literals: `@2024-01-01`, `@T12:30:00`

2. **Source Location Tracking**: Track line numbers, column positions, and character offsets for all tokens
3. **Error Context Preservation**: Maintain context information for meaningful error messages
4. **Whitespace Handling**: Properly handle and ignore whitespace while preserving location information

### Non-Functional Requirements
- **Performance**: Process 1000+ character expressions in <1ms
- **Memory Efficiency**: Minimal memory overhead for token storage
- **Error Resilience**: Continue tokenization after errors when possible
- **Extensibility**: Support future token types for SQL-on-FHIR and CQL extensions

### Acceptance Criteria
- [ ] All FHIRPath R4 tokens are correctly recognized and classified
- [ ] Source location information is accurate for all tokens
- [ ] All 934 official FHIRPath test cases tokenize successfully
- [ ] Clear error messages provided for invalid tokens
- [ ] Performance targets met (<1ms for typical expressions)
- [ ] Memory usage is efficient (minimal token object overhead)
- [ ] Code coverage >95% with comprehensive unit tests

---

## Technical Specifications

### Affected Components
- **fhir4ds/parser/lexer.py**: Main lexer implementation (new)
- **fhir4ds/parser/tokens.py**: Token definitions and types (new)
- **fhir4ds/parser/exceptions.py**: Lexer-specific exceptions (new)

### File Modifications
- **fhir4ds/parser/__init__.py**: New module initialization
- **fhir4ds/parser/lexer.py**: Complete lexer implementation
- **fhir4ds/parser/tokens.py**: Token type definitions and classes
- **fhir4ds/parser/exceptions.py**: LexerError and related exceptions
- **tests/unit/parser/test_lexer.py**: Comprehensive lexer tests

### Database Considerations
- **No database dependencies**: Pure parsing component with no database interaction

---

## Dependencies

### Prerequisites
1. **Project Structure Setup**: Basic fhir4ds module structure created
2. **Python Environment**: Python 3.11+ with development dependencies
3. **Test Framework**: Pytest and testing infrastructure setup

### Blocking Tasks
- None (first task in implementation sequence)

### Dependent Tasks
- **SP-001-002**: Parser Framework Implementation (requires lexer output)
- **SP-001-003**: AST Node Structure Design (uses token types)

---

## Implementation Approach

### High-Level Strategy
Implement a regex-based lexer using Python's `re` module for efficient token matching. Design follows clean separation between token definitions, lexer logic, and error handling. Focus on correctness first, then optimize for performance.

### Implementation Steps

1. **Token Type Definitions** (4 hours)
   - Estimated Time: 4 hours
   - Key Activities:
     - Define TokenType enum with all FHIRPath token categories
     - Create Token dataclass with type, value, and location information
     - Implement SourceLocation class for tracking position information
   - Validation: Token types cover complete FHIRPath grammar

2. **Regex Pattern Definitions** (6 hours)
   - Estimated Time: 6 hours
   - Key Activities:
     - Create comprehensive regex patterns for all token types
     - Implement pattern priority ordering (keywords before identifiers)
     - Add support for escape sequences in string literals
     - Handle date/time literal formats
   - Validation: Patterns correctly match all FHIRPath token examples

3. **Core Lexer Implementation** (8 hours)
   - Estimated Time: 8 hours
   - Key Activities:
     - Implement FHIRPathLexer class with tokenize() method
     - Add position tracking and whitespace handling
     - Implement error detection and recovery
     - Create token stream iterator interface
   - Validation: Successfully tokenizes simple FHIRPath expressions

4. **Error Handling and Edge Cases** (4 hours)
   - Estimated Time: 4 hours
   - Key Activities:
     - Handle unterminated strings and invalid characters
     - Provide clear error messages with location context
     - Implement partial tokenization for error recovery
   - Validation: Appropriate errors for invalid input with helpful messages

5. **Testing and Validation** (6 hours)
   - Estimated Time: 6 hours
   - Key Activities:
     - Create comprehensive unit test suite
     - Test against sample expressions from official test cases
     - Performance testing and optimization
     - Edge case and error condition testing
   - Validation: >95% code coverage and all tests passing

### Alternative Approaches Considered
- **ANTLR Grammar**: More powerful but adds dependency complexity
- **Hand-written Character Processing**: More control but significantly more implementation time

---

## Useful Existing Code References

### From Archived Implementation

#### Token Patterns (`archive/fhir4ds/fhirpath/parser/parser.py`)
**Lines 45-89**: Basic token regex patterns
```python
# Reusable patterns (adapt and enhance):
IDENTIFIER_PATTERN = r'[a-zA-Z_][a-zA-Z0-9_]*'
STRING_PATTERN = r"'([^'\\]|\\.)*'"
NUMBER_PATTERN = r'-?\d+(\.\d+)?'
```
**What to reuse**: Basic regex foundations
**What to improve**: More comprehensive patterns, better error handling

#### Error Context (`archive/fhir4ds/fhirpath/core/error_handling.py`)
**Lines 15-35**: Source location tracking
```python
# Reusable concept (adapt structure):
@dataclass
class SourceLocation:
    line: int
    column: int
    offset: int
    length: int
```
**What to reuse**: Location tracking approach
**What to improve**: More precise position tracking

### New Architecture Principles to Follow
1. **Immutable Tokens**: Token objects should be immutable after creation
2. **Generator Pattern**: Use generators for memory efficiency with large expressions
3. **Clean Error Hierarchy**: Custom exceptions with meaningful error context
4. **Type Safety**: Complete type hints and dataclass usage

---

## Testing Strategy

### Unit Testing
- **New Tests Required**:
  - Token type recognition for all FHIRPath grammar elements
  - Source location accuracy testing
  - Error handling for invalid input
  - Performance testing with large expressions
  - Edge cases: empty strings, special characters, Unicode
- **Coverage Target**: >95% code coverage

### Integration Testing
- **Official Test Case Tokenization**: Validate tokenization of all 934 FHIRPath R4 test cases
- **Error Message Quality**: Verify error messages are helpful and actionable

### Performance Testing
- **Simple Expressions**: `Patient.name` should tokenize in <0.1ms
- **Complex Expressions**: Long chained expressions should tokenize in <1ms
- **Memory Usage**: Token objects should have minimal memory overhead

### Manual Testing
- **Edge Case Validation**: Test with malformed expressions
- **Unicode Handling**: Test with international characters in identifiers
- **Large Expression Handling**: Test with very long FHIRPath expressions

---

## Risk Assessment

### Technical Risks
| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Regex complexity causes performance issues | Low | Medium | Profile early, optimize patterns |
| Official test cases reveal missing tokens | Medium | High | Study complete FHIRPath grammar first |
| Unicode handling complications | Low | Medium | Use Python's built-in Unicode support |

### Implementation Challenges
1. **Token Priority Ordering**: Keywords vs identifiers - ensure keywords take precedence
2. **String Literal Escaping**: Properly handle escaped characters in string literals
3. **Date/Time Format Variations**: Support all valid FHIRPath date/time formats

### Contingency Plans
- **If regex performance is poor**: Switch to character-by-character processing
- **If official test cases fail**: Incremental grammar extension based on failures
- **If memory usage is excessive**: Implement token pooling or flyweight patterns

---

## Estimation

### Time Breakdown
- **Analysis and Design**: 2 hours
- **Implementation**: 22 hours (token types: 4h, patterns: 6h, lexer: 8h, errors: 4h)
- **Testing**: 6 hours
- **Documentation**: 2 hours
- **Review and Refinement**: 2 hours
- **Total Estimate**: 34 hours (~1 week)

### Confidence Level
- [x] High (90%+ confident in estimate)
- [ ] Medium (70-89% confident)
- [ ] Low (<70% confident - needs further analysis)

### Factors Affecting Estimate
- **FHIRPath Grammar Complexity**: May discover additional token types during implementation
- **Performance Optimization**: May need additional time if initial implementation is slow
- **Test Coverage**: Comprehensive testing may reveal edge cases requiring additional work

---

## Success Metrics

### Quantitative Measures
- **Token Recognition Rate**: 100% of FHIRPath tokens correctly identified
- **Official Test Case Success**: 100% of 934 test cases tokenize successfully
- **Performance**: <1ms tokenization time for expressions up to 1000 characters
- **Code Coverage**: >95% test coverage

### Qualitative Measures
- **Code Quality**: Clean, readable implementation following Python best practices
- **Error Message Quality**: Clear, actionable error messages with source context
- **Architecture Alignment**: Clean interfaces suitable for parser integration

### Compliance Impact
- **FHIRPath Foundation**: Enables complete FHIRPath grammar parsing
- **Test Suite Readiness**: Prepares for 100% compliance validation
- **Performance Foundation**: Efficient tokenization enables fast parsing

---

## Documentation Requirements

### Code Documentation
- [x] Inline comments for complex regex patterns
- [x] Function/method documentation for all public APIs
- [x] Token type documentation with examples
- [x] Error handling documentation

### Architecture Documentation
- [ ] Token type definitions and relationships
- [ ] Lexer architecture and processing flow
- [ ] Performance characteristics documentation
- [ ] Integration guidelines for parser layer

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
- [ ] All FHIRPath token types implemented and tested
- [ ] Source location tracking working accurately
- [ ] Error handling comprehensive with good messages
- [ ] All 934 official test cases tokenize successfully
- [ ] Performance targets met (<1ms for typical expressions)
- [ ] Unit tests written with >95% coverage
- [ ] Code reviewed and approved
- [ ] Documentation completed
- [ ] Integration ready for parser implementation

---

**Task Created**: 25-01-2025
**Status**: Not Started

---

*This task establishes the foundational tokenization capability required for complete FHIRPath parsing and AST generation.*