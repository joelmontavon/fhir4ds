# PEP Draft 001: Complete Library Rewrite

```
PEP: [To be assigned]
Title: Complete Library Rewrite with Unified FHIRPath Architecture
Author: Senior Solution Architect/Engineer
Status: Draft
Type: Standard
Created: 25-01-2025
Updated: 25-01-2025
Version: 0.1
```

---

## Abstract

This PEP proposes a complete rewrite of the FHIR4DS library to implement the unified FHIRPath architecture. Rather than attempting to refactor the existing codebase, we will start fresh while strategically reusing valuable existing assets like test suites, schemas, and proven SQL patterns. The rewrite establishes FHIRPath as the single execution foundation for all healthcare expression languages (FHIRPath, SQL-on-FHIR, CQL), eliminating current architectural fragmentation. This approach enables rapid progress toward 100% specification compliance while dramatically reducing code complexity. No backward compatibility will be maintained, allowing for optimal architectural decisions without legacy constraints.

## Motivation

### Current Architectural Problems

The existing FHIR4DS codebase has fundamental architectural issues that prevent achieving 100% specification compliance:

#### Multiple Execution Paths
We currently maintain three separate execution architectures:
- SQL-on-FHIR: ViewRunner + Pipeline system (84.5% compliance)
- FHIRPath: Legacy FHIRPath class system (0.9% compliance)
- CQL: Mixed architecture patterns (varied compliance)

This fragmentation means optimizations don't propagate, debugging requires understanding multiple systems, and consistency across specifications is impossible.

#### Dialect Architecture Violations
Database dialects contain extensive business logic (89KB+ files) when they should only handle syntax differences. This violates the thin dialect principle and makes adding new database support extremely complex.

#### Over-Engineering Without Benefit
The current pipeline includes complex context modes, operation handlers, and state management that add significant complexity without proportional functionality gains.

### Why Complete Rewrite vs. Refactoring

1. **Architectural Foundation**: The multiple execution paths cannot be unified incrementally - they require a fundamentally different foundation
2. **Technical Debt**: Accumulated complexity makes incremental changes more expensive than clean implementation
3. **Specification Alignment**: Starting fresh allows direct implementation of specification requirements without legacy workarounds
4. **Development Velocity**: Clean architecture will enable much faster progress on compliance goals

## Rationale

### Design Principles

- **FHIRPath as Single Foundation**: FHIRPath is the core expression language that other specifications extend
- **Translation-Based Unification**: SQL-on-FHIR and CQL expressions translate to FHIRPath for unified execution
- **CTE-First SQL Generation**: Every FHIRPath operation maps to CTE templates for optimal database performance
- **Population-Scale Default**: Process entire patient populations, not individual patients
- **Pure Syntax Dialects**: Database differences handled only through syntax translation

### Scope Decision: Start with FHIRPath Foundation

Rather than attempting all three specifications simultaneously, this PEP proposes focusing initially on:

**Primary Scope: Complete FHIRPath R4 Implementation**
- Single specification focus enables architectural validation
- FHIRPath is the foundation layer for other specifications
- 934 official test cases provide clear success criteria
- Clean implementation without legacy constraints

**Secondary Scope: Code Reuse Strategy**
- Preserve all 197 existing test files for validation
- Extract proven SQL generation patterns from current SQL-on-FHIR implementation
- Reuse FHIR schema definitions and validation logic
- Adapt database connection and configuration management

### Why No Backward Compatibility

Maintaining backward compatibility would:
- Force architectural compromises to support legacy patterns
- Require maintaining multiple execution paths during transition
- Significantly increase implementation complexity
- Slow progress toward compliance goals
- Prevent optimal architectural decisions

## Specification

### Architecture Overview

Five-layer architecture implementing unified FHIRPath foundation:

```
Input Layer: FHIRPath expressions
↓
FHIRPath Engine: Parse, optimize, generate execution plan
↓
CTE Generator: FHIRPath operations → CTE templates
↓
SQL Assembler: CTEs → Monolithic SQL
↓
Thin Dialect Layer: Database syntax translation only
```

### Core Components

#### FHIRPathEngine
```python
class FHIRPathEngine:
    """Core FHIRPath execution engine."""

    def parse(self, expression: str) -> FHIRPathAST:
        """Parse FHIRPath expression into AST."""

    def optimize(self, ast: FHIRPathAST) -> FHIRPathAST:
        """Optimize for population-scale execution."""

    def execute(self, ast: FHIRPathAST, context: FHIRContext) -> FHIRPathResult:
        """Execute against FHIR data context."""
```

#### CTEGenerator
```python
class CTEGenerator:
    """Generate CTE chains from FHIRPath AST."""

    def generate(self, ast: FHIRPathAST) -> CTEChain:
        """Convert FHIRPath operations to CTE templates."""
```

#### SQLDialect (Thin Layer)
```python
class SQLDialect:
    """Pure syntax differences only."""

    def json_extract(self, obj: str, path: str) -> str:
        """Database-specific JSON extraction syntax."""

    def array_agg(self, expr: str) -> str:
        """Database-specific array aggregation syntax."""
```

### API Design

#### Single Entry Point
```python
from fhir4ds import FHIRPathEngine

engine = FHIRPathEngine(dialect="duckdb", connection="data.db")
result = engine.evaluate("Patient.name.given.first()", context)
```

#### Configuration
```python
@dataclass
class FHIRPathConfig:
    dialect: str  # "duckdb" or "postgresql"
    connection_string: str
    population_mode: bool = True
    debug_cte: bool = False
```

## Implementation

### Development Approach

#### Phase 1: Core FHIRPath Engine
- FHIRPath grammar and parser implementation
- AST node structure and validation
- Basic expression evaluation framework
- Unit testing for core functionality

#### Phase 2: CTE Generation System
- CTE template framework
- FHIRPath function → CTE mapping
- Population-scale SQL generation patterns
- Database dialect abstraction

#### Phase 3: Integration and Validation
- End-to-end FHIRPath expression execution
- Official test suite validation (934 tests)
- Performance optimization and benchmarking
- Documentation and examples

### Resource Requirements

- **Timeline**: 8-10 weeks for complete FHIRPath implementation
- **Team**: 1 senior developer primary, testing support
- **Infrastructure**: Existing DuckDB/PostgreSQL environments
- **Dependencies**: Minimal external dependencies, leverage existing where beneficial

### Success Criteria

- **100% FHIRPath R4 Compliance**: All 934 official test cases passing
- **Performance Target**: Population queries <5 seconds for 1M+ patients
- **Code Quality**: <50% of current codebase size with equivalent functionality
- **Architecture Validation**: Clean separation between layers, no business logic in dialects

## Impact Analysis

### Positive Impacts
- **Clean Architecture**: No legacy constraints enabling optimal design
- **Rapid Compliance Progress**: Direct specification implementation without workarounds
- **Simplified Maintenance**: Single execution path reduces complexity
- **Performance Optimization**: Population-first design enables database optimization

### Challenges
- **No Backward Compatibility**: Applications using current API will need complete updates
- **Initial Learning Curve**: New architecture requires understanding by development team
- **Integration Effort**: Existing workflows will need adaptation to new API

### Risk Mitigation
- **Preserve Test Assets**: All existing tests preserved for validation
- **Proven Pattern Reuse**: Extract successful patterns from current implementation
- **Iterative Validation**: Validate architecture decisions at each development phase

## Alternatives Considered

### Alternative 1: Incremental Refactoring
**Description**: Gradually modify existing codebase toward unified architecture

**Why Rejected**: Cannot address fundamental architectural divergence; legacy constraints prevent optimal design

### Alternative 2: Multi-Specification Parallel Rewrite
**Description**: Rewrite all three specifications (FHIRPath, SQL-on-FHIR, CQL) simultaneously

**Why Rejected**: Too broad for initial scope; FHIRPath foundation must be proven before building extensions

### Alternative 3: Fork and Modify Existing Implementation
**Description**: Create branch and heavily modify current architecture

**Why Rejected**: Technical debt and architectural constraints would persist; rewrite provides cleaner path

## Success Metrics

### Primary Success Criteria
- **FHIRPath Compliance**: 0.9% → 100% (all 934 official tests)
- **Architecture Validation**: Clean layer separation with thin dialects
- **Performance**: Sub-5-second population queries for 1M+ patients
- **Code Quality**: Significant reduction in codebase complexity

### Monitoring Approach
- **Daily**: Automated test execution against official FHIRPath test suite
- **Weekly**: Performance benchmarking and regression detection
- **Milestone Reviews**: Architecture validation and compliance progress

## Documentation Plan

### New Documentation Required
- **Architecture Guide**: Detailed explanation of unified FHIRPath architecture
- **API Documentation**: Complete API reference with examples
- **Migration Guide**: How to adapt from current implementation
- **Performance Guide**: Population-scale optimization techniques

## Timeline

| Milestone | Duration | Deliverable |
|-----------|----------|-------------|
| Architecture Foundation | 3 weeks | Core FHIRPath parser and AST |
| CTE Generation System | 3 weeks | SQL generation framework |
| Integration & Validation | 2 weeks | Complete FHIRPath implementation |
| Documentation & Polish | 1 week | Ready for next specification layer |

---

**Next Steps After Approval:**
1. Create detailed implementation plan
2. Set up new codebase structure
3. Begin FHIRPath parser implementation
4. Establish testing framework for official specification compliance

*This PEP focuses on the strategic decision to completely rewrite with FHIRPath foundation, enabling clean architecture and rapid progress toward 100% specification compliance.*