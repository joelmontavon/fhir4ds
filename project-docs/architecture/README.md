# Architecture Documentation

## Overview

This directory contains comprehensive architecture documentation for FHIR4DS, including strategic goals, technical decisions, reference materials, and visual representations of the system design.

## Directory Structure

### Core Documentation
- **[goals.md](goals.md)** - Strategic architecture goals and 100% compliance targets
- **[decisions/](decisions/)** - Architecture Decision Records (ADRs)
- **[diagrams/](diagrams/)** - System architecture diagrams and visual documentation
- **[reference/](reference/)** - Reference materials and external specification links

## Architecture Philosophy

### FHIRPath-First Foundation
FHIR4DS is built on the principle that **FHIRPath should be the single execution foundation** for all healthcare expression languages, providing:

- **Unified Execution Path**: Single engine for FHIRPath, SQL-on-FHIR, and CQL
- **Population-Scale Analytics**: Default to population queries with patient filtering when needed
- **CTE-First SQL Generation**: Every operation maps to CTE templates for optimal performance
- **Thin Database Dialects**: Database differences handled through simple syntax translation only

### Core Architectural Principles

#### 1. Population Analytics First
Design for population-scale analytics rather than processing one patient's data at a time:
- CQL execution operates on entire patient populations by default
- 10x+ performance improvements through population-scale optimization
- Natural fit for quality measures and population health analytics

#### 2. Standards Compliance Goals
**Target: 100% compliance** with all major healthcare interoperability specifications:
- **FHIRPath R4**: Complete implementation of FHIRPath specification
- **SQL-on-FHIR**: Full compatibility with SQL-on-FHIR standard
- **CQL Framework**: Complete Clinical Quality Language specification support
- **Quality Measures**: 100% eCQI Framework compliance

#### 3. Multi-Dialect Database Support
Support multiple database platforms with feature parity:
- **DuckDB**: Primary development and embedded analytics platform
- **PostgreSQL**: Production deployment and enterprise integration
- **Extensible**: Clean architecture supports additional dialects

#### 4. Monolithic Query Architecture
Quality measures implemented as monolithic queries for optimal performance:
- Complete CQL library execution in single database query
- All define statements combined into comprehensive CTE structure
- 11.8x average performance improvement validated across measures

## Strategic Goals

### Compliance Targets

| Specification | Target Compliance | Architecture Approach |
|---------------|------------------|----------------------|
| **FHIRPath R4** | 100% | **Foundation execution engine** |
| **SQL-on-FHIR** | 100% | **Translation to FHIRPath patterns** |
| **CQL Framework** | 100% | **Translation to FHIRPath with monolithic execution** |
| **Quality Measures** | 100% | **CQL-based measure calculation** |

### Performance Benchmarks
- **Population Scale**: Support 10M+ patients without performance degradation
- **Query Response**: Population queries complete within 5 seconds
- **Measure Execution**: Quality measures calculate in <30 seconds for 1M patients
- **Memory Efficiency**: Process large datasets using <8GB RAM

### Architecture Components

#### **FHIRPath Engine** (The Heart)
- Single execution engine for all specifications
- Maintains all business logic for expression evaluation
- Population-first design with patient filtering capability

#### **Language Translators** (Input Adapters)
- **ViewDefinition→FHIRPath**: Convert SQL-on-FHIR paths to FHIRPath expressions
- **CQL→FHIRPath**: Convert CQL defines to FHIRPath expressions with dependencies

#### **CTE Generator** (SQL Builder)
- Maps each FHIRPath operation to CTE template
- Generates dependency-ordered CTE chains
- Population-optimized SQL generation

#### **SQL Assembler** (Query Combiner)
- Combines multiple CTE chains into monolithic queries
- Perfect for CQL with multiple define statements
- Database engine optimization friendly

#### **Thin Dialect Layer** (Syntax Only)
- Pure database syntax differences
- No business logic whatsoever
- Simple method overrides for SQL function names

## Architecture Decision Records (ADRs)

Architecture decisions are documented in the `decisions/` directory using the ADR format:

### Current ADRs
- **ADR-001**: [Create your first ADR here]
- **ADR-002**: [Document major architectural decisions]
- **ADR-003**: [Each ADR follows standard template]

### ADR Process
1. **Identify Decision**: Significant architectural choice requiring documentation
2. **Create ADR**: Use standard ADR template in `decisions/` directory
3. **Review**: Senior Solution Architect/Engineer reviews and approves
4. **Implementation**: Decision guides implementation approach
5. **Evolution**: Update ADR status as decisions evolve

## System Architecture Overview

### Unified Execution Path
```
┌─────────────────────────────────────────────────────────────┐
│                    INPUT LAYER                              │
├─────────────────┬─────────────────┬─────────────────────────┤
│  SQL-on-FHIR    │      CQL        │      FHIRPath           │
│ ViewDefinition  │   Expression    │     Expression          │
└─────────────────┴─────────────────┴─────────────────────────┘
         │                 │                   │
         ▼                 ▼                   │
┌─────────────────┐┌─────────────────┐        │
│ViewDef→FHIRPath ││  CQL→FHIRPath   │        │
│   Translator    ││   Translator    │        │
└─────────────────┘└─────────────────┘        │
         │                 │                   │
         └─────────────────┼───────────────────┘
                          ▼
              ┌─────────────────────────┐
              │    FHIRPath Engine      │
              │  (Single Execution)     │
              └─────────────────────────┘
                          │
                          ▼
              ┌─────────────────────────┐
              │   CTE Generator         │
              │ (Expression → CTEs)     │
              └─────────────────────────┘
                          │
                          ▼
              ┌─────────────────────────┐
              │  SQL Assembler          │
              │ (CTEs → Monolithic SQL) │
              └─────────────────────────┘
                          │
                          ▼
              ┌─────────────────────────┐
              │   Thin Dialect Layer    │
              │  (Syntax Translation)   │
              └─────────────────────────┘
```

### Component Responsibilities

#### **FHIRPath Engine** (The Heart)
- Single execution engine for all specifications
- Maintains all business logic for expression evaluation
- Population-first design with patient filtering capability

#### **Language Translators** (Input Adapters)
- **ViewDefinition→FHIRPath**: Convert SQL-on-FHIR paths to FHIRPath expressions
- **CQL→FHIRPath**: Convert CQL defines to FHIRPath expressions with dependencies

#### **CTE Generator** (SQL Builder)
- Maps each FHIRPath operation to CTE template
- Generates dependency-ordered CTE chains
- Population-optimized SQL generation

#### **SQL Assembler** (Query Combiner)
- Combines multiple CTE chains into monolithic queries
- Perfect for CQL with multiple define statements
- Database engine optimization friendly

#### **Thin Dialect Layer** (Syntax Only)
- Pure database syntax differences
- No business logic whatsoever
- Simple method overrides for SQL function names

## Reference Materials

### Official Specifications
Comprehensive links to all target specifications are documented in:
- **[specifications.md](reference/specifications.md)** - Complete specification reference with official links

### Key Specifications Include:
- **FHIRPath R4**: [hl7.org/fhirpath/](https://hl7.org/fhirpath/)
- **SQL-on-FHIR v2.0**: [sql-on-fhir-v2.readthedocs.io](https://sql-on-fhir-v2.readthedocs.io/)
- **CQL R1.5**: [cql.hl7.org](https://cql.hl7.org/)
- **eCQI Framework**: [ecqi.healthit.gov](https://ecqi.healthit.gov/)

### Official Testing Resources
- **FHIRPath Test Cases**: [github.com/HL7/FHIRPath/tree/master/tests](https://github.com/HL7/FHIRPath/tree/master/tests)
- **SQL-on-FHIR Tests**: [github.com/sql-on-fhir-v2/sql-on-fhir-v2/tree/main/tests](https://github.com/sql-on-fhir-v2/sql-on-fhir-v2/tree/main/tests)
- **CQL Test Suite**: [github.com/cqframework/cql-tests](https://github.com/cqframework/cql-tests)

## Quality Assurance

### Compliance Monitoring
- **Daily Test Execution**: Automated execution of all specification test suites
- **Compliance Dashboard**: Real-time compliance metrics and trend analysis
- **Regression Detection**: Immediate notification of compliance degradation
- **Performance Monitoring**: Continuous performance benchmarking and alerting

### Testing Strategy
- **Official Test Suites**: Execute all official specification test suites
- **Custom Test Development**: FHIR4DS-specific test cases for edge conditions
- **Regression Prevention**: Comprehensive regression testing on every change
- **Performance Validation**: Population-scale performance verification

### Architecture Review Process
1. **Design Review**: All architectural changes reviewed before implementation
2. **Implementation Review**: Code review focuses on architectural alignment
3. **Compliance Verification**: Changes verified against specification requirements
4. **Performance Assessment**: Performance impact assessed and documented

## Getting Started

### For Architects
1. **Review Goals**: Start with [goals.md](goals.md) for strategic objectives
2. **Understand Principles**: Review core architectural principles above
3. **Study Decisions**: Read existing ADRs in [decisions/](decisions/) directory
4. **Plan Changes**: Create new ADRs for significant architectural decisions

### For Developers
1. **Architecture Context**: Understand the FHIRPath-first foundation
2. **Implementation Patterns**: Follow established patterns for database dialects
3. **Testing Requirements**: Ensure all changes maintain specification compliance
4. **Documentation**: Update architecture documentation with structural changes

### For Stakeholders
1. **Strategic Vision**: Review goals and compliance targets
2. **Progress Tracking**: Monitor compliance metrics and architecture evolution
3. **Quality Assurance**: Understand testing and validation approaches
4. **Reference Materials**: Access official specifications and community resources

## Contributing to Architecture Documentation

### Adding New Documentation
- **ADRs**: Create new ADRs for significant architectural decisions
- **Diagrams**: Add visual representations of system components and interactions
- **Reference Updates**: Keep specification links and testing resources current
- **Goal Tracking**: Update progress toward compliance and performance targets

### Documentation Standards
- **Clear Writing**: Use professional, unambiguous language
- **Visual Aids**: Include diagrams and examples where helpful
- **Version Control**: Track all changes with clear rationale
- **Regular Review**: Keep documentation current with system evolution

---

## Conclusion

FHIR4DS architecture is designed to achieve 100% compliance with all major healthcare interoperability specifications while delivering industry-leading performance for population-scale analytics. The FHIRPath-first foundation with CTE-based SQL generation provides a unified, optimizable execution model that scales to production healthcare analytics workloads.

This architecture documentation serves as the authoritative source for understanding system design principles, tracking strategic progress, and guiding implementation decisions.

---

*This architecture documentation is actively maintained to reflect the current state and future direction of FHIR4DS system design.*