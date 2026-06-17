# Task: Architecture Compliance Fixes

**Task ID**: SP-002-004
**Sprint**: SP-002
**Task Name**: Architecture Compliance Fixes and Clean Foundation
**Assignee**: Senior Architect
**Created**: 26-01-2025
**Last Updated**: 26-01-2025
**Dependencies**: SP-002-002 (Core Function Library)

---

## Task Overview

### Description
Remove identified architecture violations and ensure the FHIRPath parser foundation fully complies with the unified FHIR4DS architecture principles. This task addresses hardcoded values, improper exception handling, missing configuration systems, and other violations identified in the PEP-001 code review to establish a clean foundation for future development.

### Category
- [ ] Feature Implementation
- [ ] Testing Infrastructure
- [x] Architecture Enhancement
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

### Architecture Violations to Fix

#### 1. Remove Hardcoded Values (Critical Violation)
**Current Issue**:
```python
# parser.py line 67-70 - VIOLATES "No Hardcoded Values" principle
def _create_mock_metadata(self) -> PopulationMetadata:
    return PopulationMetadata(
        cardinality=Cardinality.COLLECTION,  # Hardcoded!
        fhir_type="Any"                      # Hardcoded!
    )
```

**Required Fix**: Implement proper metadata inference system based on expression context.

#### 2. Implement Specific Exception Hierarchy (Critical Violation)
**Current Issue**:
```python
# parser.py line 59 & 231 - Poor error handling
raise Exception(message)  # Generic exceptions violate architecture
```

**Required Fix**: Replace with specific parser exception types with proper error context.

#### 3. Add Configuration-Driven Behavior (Architectural Requirement)
**Current Issue**: No configuration system for parser behavior.

**Required Fix**: Implement configuration framework for parser settings and behavior.

#### 4. Improve Error Handling Consistency (Quality Issue)
**Current Issue**: Mixed error handling approaches across parser components.

**Required Fix**: Establish consistent error handling patterns throughout parser.

### Functional Requirements

1. **Metadata Inference System**:
   - Infer cardinality based on expression type (single vs collection operations)
   - Infer FHIR types based on path navigation and function usage
   - Calculate complexity scores based on expression structure
   - Track dependencies based on referenced paths

2. **Exception Hierarchy**:
   - Specific exception types for different error categories
   - Rich error context with source location and suggestions
   - Consistent error message formatting
   - Proper error propagation and handling

3. **Configuration Framework**:
   - Parser configuration for behavior settings
   - Environment-specific configuration support
   - Runtime configuration updates where appropriate
   - Validation of configuration values

4. **Error Recovery and Reporting**:
   - Consistent error handling patterns
   - Graceful degradation where possible
   - Clear error categorization and reporting
   - Developer-friendly error messages

### Non-Functional Requirements
- **No Performance Regression**: Architecture fixes must not impact parsing performance
- **Backward Compatibility**: Existing parser APIs must remain functional
- **Maintainability**: Clean, well-documented architecture patterns
- **Extensibility**: Architecture supports future enhancements

---

## Technical Specifications

### Metadata Inference System
```python
from enum import Enum
from typing import Optional, Set, Dict, Any

class MetadataInferenceEngine:
    """Intelligent metadata inference for AST nodes"""

    def infer_metadata(self, node: FHIRPathNode, context: ParseContext) -> PopulationMetadata:
        """Infer metadata based on node type and parsing context"""

        cardinality = self._infer_cardinality(node, context)
        fhir_type = self._infer_fhir_type(node, context)
        complexity = self._calculate_complexity(node)
        dependencies = self._extract_dependencies(node)

        return PopulationMetadata(
            cardinality=cardinality,
            fhir_type=fhir_type,
            complexity_score=complexity,
            dependencies=dependencies
        )

    def _infer_cardinality(self, node: FHIRPathNode, context: ParseContext) -> Cardinality:
        """Infer cardinality based on expression structure"""
        if isinstance(node, (MemberAccess, Identifier)):
            # Path navigation - check if property is collection
            return self._lookup_property_cardinality(node, context)
        elif isinstance(node, InvocationExpression):
            # Function calls - check function return cardinality
            return self._lookup_function_cardinality(node.name.value)
        elif isinstance(node, BinaryOperation):
            # Binary operations typically return single values
            return Cardinality.SINGLE
        else:
            return Cardinality.SINGLE

    def _infer_fhir_type(self, node: FHIRPathNode, context: ParseContext) -> Optional[str]:
        """Infer FHIR type based on path navigation and context"""
        # Implementation would use FHIR schema knowledge
        pass

class ParseContext:
    """Context information for metadata inference"""

    def __init__(self):
        self.resource_type: Optional[str] = None
        self.current_path: List[str] = []
        self.function_context: Optional[str] = None
        self.fhir_schema: Optional[Dict[str, Any]] = None
```

### Exception Hierarchy
```python
class FHIRPathError(Exception):
    """Base exception for all FHIRPath parsing errors"""

    def __init__(self, message: str, location: Optional[SourceLocation] = None,
                 context: Optional[str] = None, suggestion: Optional[str] = None):
        self.message = message
        self.location = location
        self.context = context
        self.suggestion = suggestion
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format error message with location and context"""
        msg = self.message
        if self.location:
            msg = f"Line {self.location.line}, Column {self.location.column}: {msg}"
        if self.context:
            msg += f"\nContext: {self.context}"
        if self.suggestion:
            msg += f"\nSuggestion: {self.suggestion}"
        return msg

class FHIRPathSyntaxError(FHIRPathError):
    """Syntax errors in FHIRPath expressions"""
    pass

class FHIRPathParseError(FHIRPathError):
    """Parsing errors (grammar violations)"""
    pass

class FHIRPathSemanticError(FHIRPathError):
    """Semantic errors (type mismatches, invalid operations)"""
    pass

class FHIRPathFunctionError(FHIRPathError):
    """Function-related errors (unknown functions, invalid arguments)"""
    pass
```

### Configuration Framework
```python
from dataclasses import dataclass
from typing import Dict, Any, Optional
import os

@dataclass
class ParserConfiguration:
    """Configuration for FHIRPath parser behavior"""

    # Performance settings
    enable_metadata_inference: bool = True
    enable_function_validation: bool = True
    max_expression_depth: int = 100

    # Error handling settings
    strict_mode: bool = False
    enable_error_recovery: bool = False
    detailed_error_messages: bool = True

    # Function settings
    enable_custom_functions: bool = True
    function_timeout_ms: int = 1000

    # Development settings
    debug_mode: bool = False
    enable_ast_validation: bool = True

class ConfigurationManager:
    """Manages parser configuration from various sources"""

    def __init__(self):
        self.config = ParserConfiguration()
        self._load_configuration()

    def _load_configuration(self):
        """Load configuration from environment and config files"""
        # Environment variables
        if os.getenv('FHIRPATH_STRICT_MODE'):
            self.config.strict_mode = os.getenv('FHIRPATH_STRICT_MODE').lower() == 'true'

        if os.getenv('FHIRPATH_DEBUG'):
            self.config.debug_mode = os.getenv('FHIRPATH_DEBUG').lower() == 'true'

        # Configuration file support
        config_file = os.getenv('FHIRPATH_CONFIG_FILE')
        if config_file and os.path.exists(config_file):
            self._load_config_file(config_file)

    def get_config(self) -> ParserConfiguration:
        """Get current parser configuration"""
        return self.config

    def update_config(self, **kwargs):
        """Update configuration at runtime"""
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
```

### Enhanced Parser Integration
```python
class Parser:
    """Enhanced parser with architecture compliance"""

    def __init__(self, tokens: list[Token], config: Optional[ParserConfiguration] = None):
        self.tokens = tokens
        self.pos = 0
        self.config = config or ConfigurationManager().get_config()
        self.metadata_engine = MetadataInferenceEngine()
        self.parse_context = ParseContext()

    def _create_metadata(self, node: FHIRPathNode) -> PopulationMetadata:
        """Create proper metadata using inference engine"""
        if self.config.enable_metadata_inference:
            return self.metadata_engine.infer_metadata(node, self.parse_context)
        else:
            # Fallback to basic metadata
            return PopulationMetadata(
                cardinality=Cardinality.UNKNOWN,
                fhir_type=None,
                complexity_score=1,
                dependencies=set()
            )

    def _raise_parse_error(self, message: str, suggestion: Optional[str] = None) -> None:
        """Raise properly formatted parse error"""
        location = self._get_current_location()
        context = self._get_error_context()
        raise FHIRPathParseError(message, location, context, suggestion)

    def _raise_syntax_error(self, expected: str, found: str) -> None:
        """Raise syntax error with helpful context"""
        message = f"Expected {expected}, found {found}"
        suggestion = f"Check for missing or extra {expected}"
        location = self._get_current_location()
        context = self._get_error_context()
        raise FHIRPathSyntaxError(message, location, context, suggestion)
```

---

## Implementation Plan

### Day 1: Exception Hierarchy and Error Handling
- **Hour 1-4**: Implement complete exception hierarchy with proper error context
- **Hour 5-8**: Replace all generic exceptions in parser with specific types
- **Hour 9-12**: Implement consistent error message formatting and suggestions
- **Hour 13-16**: Test error handling with various invalid expressions

### Day 2: Metadata Inference System
- **Hour 1-4**: Implement MetadataInferenceEngine with cardinality inference
- **Hour 5-8**: Add FHIR type inference and complexity calculation
- **Hour 9-12**: Integrate metadata inference with parser
- **Hour 13-16**: Test metadata inference across various expression types

### Day 3: Configuration Framework and Final Integration
- **Hour 1-4**: Implement configuration framework with environment support
- **Hour 5-8**: Integrate configuration with parser behavior
- **Hour 9-12**: Complete architecture compliance validation
- **Hour 13-16**: Performance testing and regression validation

---

## Acceptance Criteria

### Primary Acceptance Criteria
1. **✅ No Hardcoded Values**:
   - All hardcoded metadata replaced with proper inference
   - Configuration-driven behavior throughout parser
   - No magic numbers or hardcoded strings in parser logic

2. **✅ Proper Exception Hierarchy**:
   - All exceptions use specific types (no generic Exception)
   - Rich error context with source location and suggestions
   - Consistent error message formatting

3. **✅ Configuration Framework**:
   - Parser behavior configurable through environment variables
   - Configuration file support for complex settings
   - Runtime configuration updates supported

4. **✅ Metadata Inference**:
   - Intelligent metadata inference based on expression context
   - Cardinality inference for all expression types
   - FHIR type inference where possible

### Quality Gates
- **No performance regression**: Architecture fixes don't impact parsing speed
- **All tests pass**: No regression in existing functionality
- **Code review passes**: Architecture compliance verified
- **Error handling improved**: Better error messages and context

---

## Testing Strategy

### Architecture Compliance Testing
1. **Hardcoded Value Elimination**:
   - Static analysis to detect remaining hardcoded values
   - Verify metadata inference produces reasonable results
   - Test configuration-driven behavior

2. **Exception Hierarchy Validation**:
   - Verify all error paths use specific exception types
   - Test error message quality and context
   - Validate error location accuracy

3. **Configuration Testing**:
   - Test configuration loading from environment variables
   - Verify configuration file support
   - Test runtime configuration updates

### Regression Testing
- **Parser Functionality**: Ensure all existing parser features still work
- **Performance**: Validate no significant performance degradation
- **API Compatibility**: Ensure public APIs remain unchanged

---

## Deliverables

### Code Deliverables
1. **Exception Hierarchy**: `fhir4ds/parser/exceptions.py` (enhanced)
2. **Metadata Inference**: `fhir4ds/parser/metadata_inference.py`
3. **Configuration Framework**: `fhir4ds/parser/configuration.py`
4. **Enhanced Parser**: Updated `fhir4ds/parser/parser.py`
5. **Error Handling**: Improved error handling throughout parser

### Documentation Deliverables
1. **Architecture Compliance Report**: Verification of architecture adherence
2. **Configuration Guide**: How to configure parser behavior
3. **Error Handling Guide**: Understanding parser errors and solutions
4. **Migration Guide**: Changes from previous parser version

### Testing Deliverables
1. **Architecture Tests**: Tests validating architecture compliance
2. **Configuration Tests**: Tests for configuration framework
3. **Error Handling Tests**: Tests for improved error handling
4. **Regression Tests**: Tests ensuring no functionality regression

---

## Success Metrics

### Quantitative Metrics
- **Hardcoded Value Elimination**: 0 hardcoded values in parser code
- **Exception Specificity**: 100% of errors use specific exception types
- **Configuration Coverage**: All major parser behaviors configurable
- **Performance Maintenance**: <5% performance impact from architecture fixes

### Qualitative Metrics
- **Code Quality**: Clean, maintainable architecture patterns
- **Error Experience**: Improved error messages and debugging experience
- **Developer Experience**: Easy configuration and customization
- **Architecture Alignment**: Full compliance with unified FHIR4DS principles

---

## Dependencies and Blockers

### Dependencies
1. **SP-002-002**: Core function library (to ensure fixes don't break functions)
2. **SP-001**: Completed parser foundation
3. **Architecture Requirements**: Clear understanding of architecture violations

### Potential Blockers
1. **Complex Metadata Inference**: FHIR type inference may require complex schema knowledge
2. **Performance Impact**: Architecture changes may impact performance
3. **API Changes**: Configuration integration may require API modifications

---

## Risk Mitigation

### Technical Risks
1. **Performance Regression**:
   - **Mitigation**: Continuous performance monitoring during implementation
   - **Contingency**: Optimize metadata inference if performance degrades

2. **Breaking Changes**:
   - **Mitigation**: Maintain backward compatibility in public APIs
   - **Contingency**: Provide migration guide for any necessary API changes

3. **Complexity Increase**:
   - **Mitigation**: Keep architecture changes as simple as possible
   - **Contingency**: Defer complex features to future sprints if needed

---

**Task establishes clean, compliant architecture foundation ready for CTE generation and advanced features.**