# PEP Draft 001: Complete Library Architecture Rewrite

```
PEP: [To be assigned]
Title: Complete Library Architecture Rewrite with FHIRPath Foundation
Author: Senior Solution Architect/Engineer
Status: Draft
Type: Standard
Created: 25-01-2025
Updated: 25-01-2025
Version: 0.1
```

---

## Abstract

This PEP proposes a complete rewrite of the FHIR4DS library from scratch while strategically leveraging existing valuable code assets. The rewrite implements the unified FHIRPath architecture described in project-docs/process/architecture-overview.md, establishing FHIRPath as the single execution foundation for all healthcare expression languages (FHIRPath, SQL-on-FHIR, CQL). The new architecture eliminates current issues with multiple execution paths, dialect bloat, and over-engineered pipelines while achieving 100% specification compliance targets. This approach will reduce code complexity by ~70% while dramatically improving maintainability and performance through a CTE-first, population-optimized design.

## Motivation

### Current State Problems

The existing FHIR4DS codebase exhibits several critical architectural issues that impede progress toward 100% specification compliance:

#### 1. **Multiple Execution Paths**
Currently, we maintain three separate execution paths:
- SQL-on-FHIR: Uses FHIRDataStore + ViewRunner + Pipeline architecture
- FHIRPath: Uses legacy FHIRPath class + QuickConnect
- CQL: Uses CQL Engine with mixed architecture patterns

This fragmentation means fixes and optimizations don't propagate across specifications, maintenance effort is tripled, and achieving consistent behavior is nearly impossible.

#### 2. **Dialect Bloat with Business Logic**
Database dialect files (duckdb.py: 89KB, postgresql.py: 101KB) contain extensive business logic that should reside in the core engine. This violates the architectural principle that dialects should be thin syntax layers only.

#### 3. **Over-Engineered Pipeline Architecture**
The current pipeline includes complex context modes, operation handlers, state management, and multiple fallback mechanisms that add complexity without proportional benefit.

#### 4. **Specification Compliance Barriers**
- FHIRPath compliance: Currently minimal due to architectural mismatch
- Each specification requires separate implementation effort
- Testing and validation must be replicated across execution paths

### Expected Benefits of Rewrite

#### 1. **Unified Architecture Implementation**
- Single FHIRPath execution foundation for all specifications
- CTE-first SQL generation optimized for population analytics
- Thin dialect layer with only syntax differences
- Dramatic reduction in code duplication and complexity

#### 2. **100% Specification Compliance Path**
- Clear architectural alignment with all target specifications
- Systematic testing approach against official test suites
- Elimination of specification-specific execution barriers

#### 3. **Performance and Maintainability**
- Population-scale optimization by default
- Monolithic SQL generation for maximum database optimization
- Clean, testable codebase with clear separation of concerns

### Use Cases

1. **Use Case 1: Healthcare Analytics Organization**
   - Current behavior: Must choose between different execution paths with varying compliance levels
   - Proposed behavior: Single, unified API with 100% specification compliance across all standards
   - Benefit: Consistent, reliable results with simplified integration

2. **Use Case 2: Quality Measure Development**
   - Current behavior: CQL measures require separate implementation and optimization strategies
   - Proposed behavior: CQL measures translate to FHIRPath and execute as optimized monolithic SQL
   - Benefit: 10x+ performance improvement with simplified debugging

3. **Use Case 3: Multi-Database Deployment**
   - Current behavior: Dialect differences include business logic requiring extensive testing
   - Proposed behavior: Thin dialects with only syntax differences enable rapid database support
   - Benefit: Easy database platform migration and consistent behavior

## Rationale

### Design Principles

- **FHIRPath-First Foundation**: FHIRPath is the simplest and most fundamental healthcare expression language, making it the optimal foundation layer
- **Translation-Based Unification**: SQL-on-FHIR and CQL translate to FHIRPath expressions, enabling single execution path
- **CTE-First SQL Generation**: Every FHIRPath operation maps to CTE templates for optimal database performance
- **Population-Scale Analytics**: Default to processing entire patient populations rather than row-by-row operations
- **Thin Dialect Separation**: Database differences handled purely through syntax translation without business logic

### Why Complete Rewrite vs. Incremental Refactoring

1. **Architectural Fundamentals**: Current architecture has divergent execution paths that cannot be unified incrementally
2. **Technical Debt**: Accumulated complexity makes incremental changes more expensive than clean implementation
3. **Testing Strategy**: Fresh implementation allows systematic specification compliance validation from the start
4. **Code Quality**: Starting from unified architecture ensures consistent patterns throughout

### Architecture Approach Justification

The proposed architecture directly implements patterns proven successful in the current SQL-on-FHIR implementation (84.5% compliance) while addressing FHIRPath and CQL execution through the same foundation.

## Specification

### Overview

The rewritten library implements a five-layer architecture:

```
1. Input Layer: SQL-on-FHIR, CQL, FHIRPath expressions
2. Translation Layer: Convert to FHIRPath AST
3. FHIRPath Engine: Core execution logic
4. CTE Generator: FHIRPath operations → CTE templates
5. SQL Assembler: CTEs → Monolithic SQL
6. Thin Dialect Layer: Database syntax translation only
```

### Core Components

#### FHIRPathEngine
```python
class FHIRPathEngine:
    """Core FHIRPath execution engine - single foundation for all specifications."""

    def parse(self, expression: str) -> FHIRPathAST:
        """Parse FHIRPath string into AST."""

    def optimize(self, ast: FHIRPathAST) -> FHIRPathAST:
        """Optimize AST for population-scale execution."""

    def generate_cte_plan(self, ast: FHIRPathAST) -> CTEPlan:
        """Generate CTE execution plan from optimized AST."""
```

#### Translation Layer
```python
class ViewDefinitionTranslator:
    """Converts SQL-on-FHIR ViewDefinitions to FHIRPath expressions."""

    def translate(self, view_def: ViewDefinition) -> List[FHIRPathExpression]:
        """Convert ViewDefinition columns to FHIRPath expressions."""

class CQLTranslator:
    """Converts CQL expressions to FHIRPath expressions with dependency resolution."""

    def translate(self, cql_library: CQLLibrary) -> List[FHIRPathExpression]:
        """Convert CQL defines to FHIRPath expressions with dependency graph."""
```

#### CTE Generator
```python
class CTEGenerator:
    """Generates CTE chains from FHIRPath expressions."""

    def generate(self, expressions: List[FHIRPathAST]) -> CTEChain:
        """Convert FHIRPath AST to dependency-ordered CTE chain."""
```

#### Thin Dialect Layer
```python
class SQLDialect:
    """Base class for database-specific syntax only."""

    def json_extract(self, obj: str, path: str) -> str:
        """Database-specific JSON extraction syntax."""
        raise NotImplementedError

    def json_array_agg(self, expr: str) -> str:
        """Database-specific JSON array aggregation syntax."""
        raise NotImplementedError
```

### API Changes

#### New Unified API
```python
from fhir4ds import FHIR4DS

# Single entry point for all specifications
engine = FHIR4DS(dialect="duckdb", connection_string="data.db")

# FHIRPath expressions
result = engine.evaluate_fhirpath("Patient.name.given.first()")

# SQL-on-FHIR ViewDefinitions
result = engine.evaluate_viewdefinition(view_def)

# CQL Libraries
result = engine.evaluate_cql_library(cql_library)
```

#### Deprecated APIs
All current specification-specific entry points will be deprecated with 6-month migration timeline:
- Current FHIRPath classes
- Current SQL-on-FHIR ViewRunner
- Current CQL Engine classes

### Configuration Changes

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dialect` | string | `"duckdb"` | Database dialect (duckdb, postgresql) |
| `connection_string` | string | Required | Database connection string |
| `population_mode` | boolean | `true` | Enable population-scale optimization |
| `debug_cte` | boolean | `false` | Include CTE debugging information |
| `compliance_mode` | string | `"strict"` | Specification compliance level |

### Data Model Changes

#### Unified Resource Storage
- Single `fhir_resources` table for all FHIR resources
- Standardized JSON structure across database dialects
- Optimized indexing strategy for population queries

#### CTE Metadata Tables
- `cte_execution_plans` for performance monitoring
- `specification_compliance` for tracking test results

## Implementation

### Development Plan

#### Phase 1: Foundation Architecture (Weeks 1-3)
- [ ] **Week 1**: Core FHIRPath parser and AST implementation
  - Implement FHIRPath grammar and tokenizer
  - Build AST node structure
  - Create basic expression validation
  - Unit tests for parsing functionality

- [ ] **Week 2**: CTE generator framework
  - Design CTE template system
  - Implement basic FHIRPath function mappings
  - Create CTE dependency resolution
  - Population-scale SQL generation patterns

- [ ] **Week 3**: Thin dialect layer implementation
  - Extract pure syntax differences from existing dialects
  - Implement DuckDB and PostgreSQL dialect classes
  - Eliminate all business logic from dialect layer
  - Dialect compatibility test suite

#### Phase 2: Translation Layer (Weeks 4-6)
- [ ] **Week 4**: SQL-on-FHIR translation
  - ViewDefinition to FHIRPath expression conversion
  - Leverage existing SQL-on-FHIR compliance patterns
  - Maintain 84.5% compliance level as baseline

- [ ] **Week 5**: CQL translation foundation
  - CQL AST to FHIRPath expression mapping
  - Dependency resolution for CQL defines
  - Basic CQL function library mapping

- [ ] **Week 6**: Translation validation
  - End-to-end translation testing
  - Cross-specification consistency validation
  - Performance baseline establishment

#### Phase 3: Core Engine Integration (Weeks 7-9)
- [ ] **Week 7**: FHIRPath engine optimization
  - Population-scale optimization algorithms
  - CTE plan generation and optimization
  - Memory-efficient AST processing

- [ ] **Week 8**: SQL assembler implementation
  - Monolithic SQL generation from CTE plans
  - Cross-CTE optimization strategies
  - Database-specific optimization hints

- [ ] **Week 9**: End-to-end integration testing
  - Full specification compliance testing
  - Performance validation against existing implementation
  - Multi-database compatibility verification

#### Phase 4: Migration and Validation (Weeks 10-12)
- [ ] **Week 10**: API compatibility layer
  - Backward compatibility wrappers for existing APIs
  - Migration tooling and documentation
  - Deprecation warnings and guidance

- [ ] **Week 11**: Comprehensive testing
  - All 197 existing test files validation
  - Official specification test suites execution
  - Compliance metrics validation and reporting

- [ ] **Week 12**: Documentation and deployment preparation
  - Complete API documentation
  - Migration guides and examples
  - Performance benchmarking and optimization guides

### Resource Requirements

- **Development Time**: 12 weeks (3 months)
- **Developer Resources**: 1 full-time senior developer + 1 part-time junior developer for testing
- **Infrastructure**: Existing DuckDB and PostgreSQL test environments
- **Third-party Dependencies**: Minimal - leverage existing parsing libraries where appropriate

### Testing Strategy

#### Specification Compliance Testing
- Execute all official FHIRPath R4 test cases (934 tests)
- Execute all SQL-on-FHIR test cases (129 tests)
- Execute representative CQL test cases
- Target: 100% compliance across all specifications

#### Performance Testing
- Population-scale benchmarks with 1M+ patient datasets
- Comparison against existing implementation performance
- Memory usage and scalability validation
- Database optimization effectiveness measurement

#### Integration Testing
- End-to-end workflow testing across all three specifications
- Multi-database compatibility validation
- API backward compatibility verification
- Migration path validation

### Code Reuse Strategy

#### Valuable Existing Assets to Leverage
1. **Test Suite (197 test files)**: Complete preservation and validation
2. **FHIR Schema Definitions**: Reuse existing schema validation logic
3. **SQL-on-FHIR Patterns**: Extract successful patterns from current 84.5% compliance
4. **Database Connection Management**: Reuse existing connection infrastructure
5. **Configuration System**: Adapt existing configuration patterns

#### Assets to Replace
1. **Current Pipeline Architecture**: Replace with FHIRPath-first design
2. **Dialect Business Logic**: Extract to core engine, leave only syntax
3. **Multiple Execution Paths**: Consolidate into single FHIRPath foundation
4. **Complex State Management**: Simplify with CTE-based stateless processing

## Impact Analysis

### Backwards Compatibility

#### Breaking Changes
- All current specification-specific APIs will be deprecated
- Database schema changes for unified resource storage
- Configuration parameter restructuring

#### Migration Strategy
- 6-month parallel operation with deprecation warnings
- Automated migration tooling for common use cases
- Comprehensive migration documentation and examples
- Direct migration support for complex implementations

### Performance Impact

| Metric | Current | Expected | Improvement |
|--------|---------|----------|-------------|
| FHIRPath Compliance | 0.9% | 100% | 111x improvement |
| SQL-on-FHIR Compliance | 84.5% | 100% | 18% improvement |
| CQL Performance | Baseline | 10x faster | Population optimization |
| Code Complexity | Baseline | 70% reduction | Unified architecture |
| Maintenance Effort | Baseline | 60% reduction | Single execution path |

### Security Considerations

- **Reduced Attack Surface**: Simplified architecture reduces potential vulnerabilities
- **Input Validation**: Unified validation through single FHIRPath parser
- **SQL Injection Prevention**: Parameterized queries through CTE generation
- **Configuration Security**: Centralized configuration validation

### User Experience Impact

#### Positive Impacts
- **Simplified API**: Single entry point for all specifications
- **Consistent Behavior**: Unified execution ensures consistent results
- **Better Performance**: Population-scale optimization improves response times
- **Improved Debugging**: CTE-based SQL is more inspectable

#### Migration Requirements
- **API Updates**: Applications using current APIs will need updates
- **Configuration Changes**: Updated configuration parameters
- **Testing Validation**: Existing implementations should validate against new API

## Alternatives Considered

### Alternative 1: Incremental Refactoring
**Description**: Gradually refactor existing codebase toward unified architecture

**Pros**:
- Lower immediate risk
- Gradual migration path
- Preserves existing optimizations

**Cons**:
- Cannot address fundamental architectural divergence
- Technical debt continues to accumulate
- Multiple execution paths remain problematic
- Significantly longer timeline to achieve compliance goals

**Why Rejected**: The architectural problems are fundamental and cannot be addressed incrementally

### Alternative 2: Specification-Specific Optimization
**Description**: Optimize each specification implementation separately

**Pros**:
- Focused improvements per specification
- Lower complexity per implementation
- Faster short-term wins

**Cons**:
- Continues maintenance burden multiplication
- No unified optimization benefits
- Misses population-scale analytics opportunities
- Cannot achieve architectural consistency

**Why Rejected**: Perpetuates current problems without addressing root causes

### Status Quo (Do Nothing)
**Description**: Continue with current multi-path architecture

**Pros**:
- No development investment required
- No migration risk

**Cons**:
- Compliance goals remain unachievable
- Maintenance burden continues to increase
- Performance optimization opportunities missed
- Technical debt compounds over time

**Why Rejected**: Current architecture cannot achieve 100% specification compliance goals

## Success Metrics

### Primary Metrics
- **FHIRPath Compliance**: 0.9% → 100% by Month 3
- **SQL-on-FHIR Compliance**: 84.5% → 100% by Month 3
- **CQL Compliance**: Baseline → 100% by Month 3
- **Code Base Size**: Baseline → 70% reduction by Month 3

### Secondary Metrics
- **Test Suite Pass Rate**: 100% of existing 197 test files
- **Performance Benchmarks**: 10x improvement in population queries
- **Developer Onboarding**: 50% reduction in ramp-up time
- **Bug Resolution**: 60% reduction in specification-related issues

### Monitoring Plan
- **Daily**: Automated compliance test execution against all specification test suites
- **Weekly**: Performance benchmarking and regression detection
- **Monthly**: Code complexity and maintainability metrics
- **Quarterly**: User feedback and adoption metrics

## Timeline

| Milestone | Date | Owner | Dependencies |
|-----------|------|-------|--------------|
| PEP Approval | Week 0 | Senior Solution Architect | Review process completion |
| Foundation Complete | Week 3 | Senior Developer | PEP approval |
| Translation Layer Complete | Week 6 | Senior Developer | Foundation milestone |
| Core Integration Complete | Week 9 | Senior Developer | Translation milestone |
| Migration Ready | Week 12 | Development Team | All development complete |

## References

### Internal Documents
- [Architecture Overview](../process/architecture-overview.md) - Target architecture specification
- [Goals](../architecture/goals.md) - 100% compliance targets
- [Coding Standards](../process/coding-standards.md) - Implementation standards
- [PEP Process](../process/pep-process.md) - Development workflow

### External Specifications
- [FHIRPath R4 Specification](https://hl7.org/fhirpath/)
- [SQL-on-FHIR v2.0](https://sql-on-fhir-v2.readthedocs.io/)
- [Clinical Quality Language (CQL) R1.5](https://cql.hl7.org/)

---

## Implementation Notes

### Leveraging Existing Code Assets

#### Test Suite Preservation (High Priority)
```python
# Existing 197 test files provide comprehensive validation
# Must ensure 100% compatibility with new implementation
tests/
├── official/ (Official specification tests)
├── unit/ (Component tests)
├── integration/ (End-to-end tests)
```

#### SQL Pattern Extraction (High Priority)
```python
# Extract successful patterns from SQL-on-FHIR implementation
# Current 84.5% compliance provides proven foundation
# Focus on population-scale query patterns
```

#### Schema and Configuration Reuse (Medium Priority)
```python
# Adapt existing FHIR schema validation
# Reuse database connection management patterns
# Preserve configuration flexibility
```

### Development Risk Mitigation

#### Parallel Development Strategy
- Maintain existing implementation during rewrite
- Side-by-side validation of new architecture
- Gradual migration with fallback capabilities

#### Incremental Validation
- Validate each architecture layer independently
- Cross-reference with existing test results
- Performance benchmark at each milestone

#### Community Engagement
- Regular progress updates and feedback collection
- Early preview versions for stakeholder validation
- Documentation and examples throughout development

---

*This PEP establishes the foundation for achieving FHIR4DS's goal of 100% specification compliance through a unified, maintainable, and performance-optimized architecture.*