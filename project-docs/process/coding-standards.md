# Coding Standards

**Document Version**: 1.0
**Date**: 2025-01-21
**Status**: Development Standards

---

## Overview

This document defines the coding standards and conventions for FHIR4DS development. These standards ensure code quality, maintainability, and architectural consistency across the project.

## Core Coding Principles

### 1. Simplicity is Paramount
Your primary goal is to make the simplest possible change. Every modification should be small, targeted, and impact as little code as possible.

### 2. Address Root Causes
Always fix the root cause of any issues; never apply surface-level fixes or band-aid solutions.

### 3. No Hardcoded Values
Eliminate hardcoded values throughout the system for maximum flexibility and maintainability.

### 4. Thin Dialects Only - No Business Logic
**DIALECTS MUST CONTAIN ONLY SYNTAX DIFFERENCES - NO BUSINESS LOGIC WHATSOEVER.**

Database dialects handle only syntax translation. All business logic belongs in the FHIRPath engine and CTE generator. This is a core principle of the unified architecture.

---

## Architecture-Specific Standards

### Population Analytics First
Design for population-scale analytics rather than processing one patient's data at a time.

**Correct Approach:**
```python
# Population-first design
def calculate_measure_population(patients: List[Patient]) -> MeasureResults:
    # Single query processes entire population
    query = generate_population_query(measure_definition)
    return execute_query(query, patients)
```

**Incorrect Approach:**
```python
# Avoid row-by-row processing
def calculate_measure_individual(patients: List[Patient]) -> MeasureResults:
    results = []
    for patient in patients:  # Anti-pattern
        result = process_single_patient(patient)
        results.append(result)
    return results
```

### CTE-First SQL Generation
Use Common Table Expressions (CTEs) whenever possible for query organization and improved efficiency.

**Correct Approach:**
```python
def generate_cte_query(defines: List[CQLDefine]) -> str:
    ctes = []
    for define in defines:
        cte = f"{define.name} AS ({generate_define_sql(define)})"
        ctes.append(cte)

    return f"WITH {', '.join(ctes)} SELECT * FROM final_results"
```

### Thin Dialect Pattern (Unified Architecture)

**Thin Dialect Interface (ONLY syntax differences):**
```python
class SQLDialect:
    def json_extract(self, obj: str, path: str) -> str:
        """Database-specific JSON extraction - SYNTAX ONLY."""
        raise NotImplementedError

    def json_array_agg(self, expr: str) -> str:
        """Database-specific JSON array aggregation - SYNTAX ONLY."""
        raise NotImplementedError

    def with_clause(self, name: str, query: str) -> str:
        """Database-specific WITH clause syntax - SYNTAX ONLY."""
        return f"{name} AS ({query})"  # Usually same across databases
```

**Correct Thin Implementation:**
```python
class DuckDBDialect(SQLDialect):
    def json_extract(self, obj: str, path: str) -> str:
        return f"json_extract({obj}, '{path}')"

    def json_array_agg(self, expr: str) -> str:
        return f"json_group_array({expr})"

class PostgreSQLDialect(SQLDialect):
    def json_extract(self, obj: str, path: str) -> str:
        return f"jsonb_extract_path_text({obj}, '{path}')"

    def json_array_agg(self, expr: str) -> str:
        return f"json_agg({expr})"
```

**Anti-Pattern (Business Logic in Dialect):**
```python
# NEVER DO THIS - Business logic belongs in FHIRPath engine
def extract_json_path(self, base_expr, json_path, context_mode):
    if context_mode == COLLECTION:
        if json_path.startswith('$[*].') and len(json_path) > 5:
            field_name = json_path[5:]
            # 20+ lines of complex business logic...
            # This belongs in FHIRPath engine, not dialect!
```

---

## Code Quality Standards

### Function Design
- **Single Responsibility**: Each function should have one clear, focused purpose
- **Pure Functions**: Prefer pure functions without side effects when possible
- **Error Handling**: Comprehensive error handling with meaningful error messages
- **Type Hints**: Use type hints for all function parameters and return values

**Example:**
```python
from typing import List, Dict, Any, Optional

def extract_fhir_path(
    resource: Dict[str, Any],
    path: str,
    default: Optional[Any] = None
) -> Any:
    """Extract value from FHIR resource using FHIRPath expression.

    Args:
        resource: FHIR resource as dictionary
        path: FHIRPath expression string
        default: Default value if path not found

    Returns:
        Extracted value or default

    Raises:
        FHIRPathError: If path expression is invalid
    """
    try:
        return evaluate_fhirpath(resource, path)
    except Exception as e:
        if default is not None:
            return default
        raise FHIRPathError(f"Failed to evaluate path '{path}': {e}")
```

### Class Design
- **Clear Interface**: Public methods should have clear, intuitive interfaces
- **Dependency Injection**: Use dependency injection for external dependencies
- **Abstract Base Classes**: Define clear contracts through abstract base classes
- **Composition over Inheritance**: Prefer composition when possible

**Example:**
```python
from abc import ABC, abstractmethod
from typing import Protocol

class QueryGenerator(ABC):
    """Abstract base for SQL query generators."""

    @abstractmethod
    def generate_select(self, columns: List[str], table: str) -> str:
        """Generate SELECT statement for dialect."""
        pass

class FHIRPathEvaluator:
    """Evaluates FHIRPath expressions against FHIR resources."""

    def __init__(self, query_generator: QueryGenerator):
        self._query_generator = query_generator

    def evaluate(self, expression: str, resource: Dict[str, Any]) -> Any:
        """Evaluate FHIRPath expression."""
        # Implementation using injected query generator
        pass
```

### Variable and Method Naming
- **Descriptive Names**: Use clear, descriptive names that explain intent
- **Consistent Conventions**: Follow Python PEP 8 naming conventions
- **No Abbreviations**: Avoid abbreviations unless they are industry standard
- **Boolean Prefixes**: Use `is_`, `has_`, `can_` prefixes for boolean variables

**Examples:**
```python
# Good naming
def is_valid_fhir_resource(resource: Dict[str, Any]) -> bool:
    return "resourceType" in resource

def extract_patient_demographics(patient: Dict[str, Any]) -> PatientInfo:
    return PatientInfo(
        patient_id=patient.get("id"),
        birth_date=extract_birth_date(patient),
        gender=patient.get("gender")
    )

# Bad naming
def chk_res(r):  # Unclear abbreviations
    return "resourceType" in r

def get_stuff(p):  # Non-descriptive
    return p.get("id"), p.get("birthDate")
```

---

## Testing Standards

### Test Coverage Requirements
- **Minimum Coverage**: 90% code coverage across all modules
- **Unit Tests**: Every public method must have comprehensive unit tests
- **Integration Tests**: All component interactions must be tested
- **Dialect Testing**: All functionality must pass in both DuckDB and PostgreSQL

### Test Structure
```python
import pytest
from typing import Dict, Any

class TestFHIRPathEvaluator:
    """Test suite for FHIRPath evaluation functionality."""

    @pytest.fixture
    def sample_patient(self) -> Dict[str, Any]:
        """Sample patient resource for testing."""
        return {
            "resourceType": "Patient",
            "id": "patient-1",
            "name": [{"given": ["John"], "family": "Doe"}],
            "birthDate": "1990-01-01"
        }

    def test_extract_patient_name_success(self, sample_patient):
        """Test successful extraction of patient name."""
        evaluator = FHIRPathEvaluator()
        result = evaluator.evaluate("name[0].given[0]", sample_patient)
        assert result == "John"

    def test_extract_invalid_path_raises_error(self, sample_patient):
        """Test that invalid FHIRPath raises appropriate error."""
        evaluator = FHIRPathEvaluator()
        with pytest.raises(FHIRPathError):
            evaluator.evaluate("invalid.path.expression", sample_patient)

    @pytest.mark.parametrize("dialect", ["duckdb", "postgresql"])
    def test_database_compatibility(self, dialect, sample_patient):
        """Test functionality across database dialects."""
        evaluator = create_evaluator_for_dialect(dialect)
        result = evaluator.evaluate("id", sample_patient)
        assert result == "patient-1"
```

### Test Categories

#### Unit Tests
- Test individual functions in isolation
- Mock external dependencies
- Focus on edge cases and error conditions
- Fast execution (<1 second per test)

#### Integration Tests
- Test component interactions
- Use real database connections
- Test end-to-end workflows
- Validate data transformations

#### Compliance Tests
- Execute official specification test suites
- Validate against reference implementations
- Measure specification compliance percentages
- Prevent compliance regression

---

## Error Handling Standards

### Exception Hierarchy
```python
class FHIR4DSError(Exception):
    """Base exception for FHIR4DS errors."""
    pass

class FHIRPathError(FHIR4DSError):
    """FHIRPath evaluation errors."""
    pass

class CQLError(FHIR4DSError):
    """CQL processing errors."""
    pass

class DatabaseError(FHIR4DSError):
    """Database operation errors."""
    pass

class ComplianceError(FHIR4DSError):
    """Specification compliance errors."""
    pass
```

### Error Context
```python
def process_cql_library(library_content: str) -> CQLLibrary:
    """Process CQL library content into executable form."""
    try:
        parsed = parse_cql(library_content)
        return compile_cql(parsed)
    except ParseError as e:
        raise CQLError(f"Failed to parse CQL library: {e}") from e
    except CompilationError as e:
        raise CQLError(f"Failed to compile CQL library: {e}") from e
    except Exception as e:
        raise FHIR4DSError(f"Unexpected error processing CQL library: {e}") from e
```

---

## Documentation Standards

### Code Documentation
- **Docstrings**: All public functions and classes must have comprehensive docstrings
- **Type Information**: Document parameter and return types
- **Examples**: Include usage examples for complex functions
- **Error Documentation**: Document all exceptions that can be raised

### Inline Comments
- **Complex Logic**: Explain non-obvious business logic
- **Algorithm Explanation**: Document complex algorithms and calculations
- **Performance Notes**: Document performance considerations
- **TODO Comments**: Use TODO comments for future improvements

```python
def calculate_age_at_measurement(birth_date: str, measurement_date: str) -> int:
    """Calculate patient age at time of measurement.

    Uses FHIR date precision rules for age calculation.
    Handles partial dates (year-only, year-month) according to specification.

    Args:
        birth_date: Patient birth date in FHIR date format (YYYY, YYYY-MM, or YYYY-MM-DD)
        measurement_date: Measurement date in same format

    Returns:
        Age in complete years at measurement date

    Raises:
        ValueError: If date formats are invalid or inconsistent

    Example:
        >>> calculate_age_at_measurement("1990-05-15", "2025-01-21")
        34
    """
    # Convert FHIR dates to Python date objects, handling partial dates
    birth = parse_fhir_date(birth_date)  # Handles YYYY, YYYY-MM, YYYY-MM-DD
    measurement = parse_fhir_date(measurement_date)

    # Calculate age using date arithmetic
    # TODO: Add support for time-of-day precision in future version
    age = measurement.year - birth.year

    # Adjust for birth month/day if measurement hasn't reached birthday yet
    if (measurement.month, measurement.day) < (birth.month, birth.day):
        age -= 1

    return age
```

---

## Performance Standards

### Query Optimization
- **Population Queries**: Default to population-scale operations
- **CTE Usage**: Use CTEs for complex query organization
- **Index Awareness**: Design queries to leverage database indexes
- **Minimal Data Transfer**: Only select necessary columns and rows

### Memory Management
- **Streaming**: Use streaming for large datasets when possible
- **Generator Functions**: Prefer generators over lists for large data
- **Resource Cleanup**: Properly close database connections and file handles
- **Memory Profiling**: Profile memory usage for data-intensive operations

### Performance Monitoring
```python
import time
import logging
from contextlib import contextmanager

@contextmanager
def performance_monitor(operation_name: str):
    """Context manager for monitoring operation performance."""
    start_time = time.time()
    try:
        yield
    finally:
        duration = time.time() - start_time
        logging.info(f"Operation {operation_name} completed in {duration:.2f}s")

        # Alert if operation exceeds performance targets
        if duration > 5.0:  # 5 second threshold
            logging.warning(f"Performance threshold exceeded for {operation_name}")

# Usage
def calculate_population_measure(patients: List[Patient]) -> MeasureResults:
    """Calculate quality measure for patient population."""
    with performance_monitor("population_measure_calculation"):
        return process_measure_calculation(patients)
```

---

## Security Standards

### Data Protection
- **No Logging of PHI**: Never log protected health information
- **Input Validation**: Validate all external inputs
- **SQL Injection Prevention**: Use parameterized queries
- **Access Control**: Implement proper access controls for sensitive operations

```python
def execute_fhir_query(query: str, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Execute FHIR query with proper security measures."""
    # Input validation
    validate_query_syntax(query)
    validate_parameters(parameters)

    # Use parameterized query to prevent SQL injection
    safe_query = prepare_statement(query)

    # Execute with audit logging (no PHI in logs)
    logging.info(f"Executing FHIR query with {len(parameters)} parameters")

    try:
        return execute_parameterized_query(safe_query, parameters)
    except Exception as e:
        # Log error without exposing sensitive data
        logging.error(f"Query execution failed: {type(e).__name__}")
        raise
```

---

## Git and Version Control Standards

### Commit Standards
- **Conventional Commits**: Use conventional commit format
- **Atomic Commits**: Each commit represents one logical change
- **Clear Messages**: Write descriptive commit messages
- **Reference Issues**: Link commits to relevant issues or PEPs

**Commit Format:**
```
type(scope): brief description

Detailed explanation if needed.

- Bullet points for multiple changes
- Reference to PEP-XXX or Issue #YYY
```

**Examples:**
```
feat(fhirpath): implement lowBoundary and highBoundary functions

Add boundary functions for date/time precision handling.
Supports both date and datetime input formats.

- Add abstract methods to base dialect
- Implement DuckDB and PostgreSQL variants
- Preserve input format in boundary calculations
- Add comprehensive test coverage

Resolves: PEP-001, Issue #123
```

### Branch Naming
- **Feature branches**: `feature/PEP-XXX-brief-description`
- **Bug fixes**: `fix/issue-XXX-brief-description`
- **Process improvements**: `process/brief-description`

### Code Review Requirements
- **All changes** require code review by Senior Solution Architect/Engineer
- **No direct commits** to main/develop branches
- **All tests must pass** before review approval
- **Documentation updates** must accompany code changes

---

## Quality Gates

### Pre-Commit Checklist
- [ ] Code passes all linting and formatting checks
- [ ] All tests pass in both DuckDB and PostgreSQL environments
- [ ] Code coverage meets 90% minimum requirement
- [ ] No hardcoded values introduced
- [ ] Documentation updated for public API changes
- [ ] Security review completed for sensitive changes

### Pre-Release Checklist
- [ ] All official specification test suites passing
- [ ] Performance benchmarks met or improved
- [ ] Security scan completed with no critical issues
- [ ] Documentation complete and reviewed
- [ ] Migration scripts tested (if applicable)
- [ ] Compliance metrics validated

---

## Configuration Management

### Configuration Files
- **External Configuration**: All configuration in external files
- **Environment-Specific**: Support for dev/test/prod environments
- **Validation**: Validate configuration at startup
- **Documentation**: Document all configuration options

**Example:**
```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    dialect: str
    connection_string: str
    pool_size: int = 5
    timeout_seconds: int = 30

    def validate(self) -> None:
        """Validate configuration values."""
        if self.dialect not in ["duckdb", "postgresql"]:
            raise ValueError(f"Unsupported dialect: {self.dialect}")

        if self.pool_size < 1:
            raise ValueError("Pool size must be positive")

@dataclass
class FHIR4DSConfig:
    """Main application configuration."""
    database: DatabaseConfig
    performance_monitoring: bool = True
    log_level: str = "INFO"

    @classmethod
    def from_file(cls, config_path: str) -> 'FHIR4DSConfig':
        """Load configuration from file."""
        # Implementation to load from JSON/YAML/TOML
        pass
```

---

## Conclusion

These coding standards ensure that FHIR4DS maintains high quality, performance, and maintainability while progressing toward 100% specification compliance. All team members must follow these standards, and they will be enforced through code review and automated quality gates.

Regular review and updates of these standards ensure they continue to serve the project's evolving needs while maintaining architectural consistency and code quality.

---

*These standards are living documents that evolve with the project while maintaining core principles of simplicity, quality, and architectural alignment.*