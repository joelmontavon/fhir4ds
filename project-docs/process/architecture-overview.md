## Proposed Architecture

### Core Principle: **FHIRPath-First, CTE-Based, Population-Optimized**

```
┌─────────────────────────────────────────────────────────────┐
│                    INPUT LAYER                              │
├─────────────────┬─────────────────┬─────────────────────────┤
│  SQL-on-FHIR    │      CQL        │      FHIRPath           │
│ ViewDefinition  │   Expression    │     Expression          │
└─────────────────┘─────────────────┘─────────────────────────┘
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
                          │
                          ▼
              ┌─────────────────────────┐
              │  Database Execution     │
              │ (Population Results)    │
              └─────────────────────────┘
```

### Architecture Layers

#### **Layer 1: Input Translators**
Responsible for converting domain-specific languages to FHIRPath AST.

**ViewDefinition Translator**:
```python
class ViewDefinitionTranslator:
    """Converts SQL-on-FHIR ViewDefinitions to FHIRPath expressions."""

    def translate(self, view_def: ViewDefinition) -> List[FHIRPathExpression]:
        """
        Converts ViewDefinition columns to FHIRPath expressions.

        Example:
        {"name": "patient_name", "path": "name.family.first()"}
        → FHIRPathExpression("name.family.first()")
        """
        pass
```

**CQL Translator**:
```python
class CQLTranslator:
    """Converts CQL expressions to FHIRPath expressions."""

    def translate(self, cql_ast: CQLAST) -> List[FHIRPathExpression]:
        """
        Converts CQL define statements to FHIRPath expressions.

        Example:
        define "HasDiabetes": exists([Condition: "Diabetes"])
        → FHIRPathExpression with dependency graph
        """
        pass
```

#### **Layer 2: FHIRPath Engine (Core)**
Single execution engine for all FHIRPath expressions.

```python
class FHIRPathEngine:
    """Core FHIRPath execution engine - the heart of the system."""

    def parse(self, expression: str) -> FHIRPathAST:
        """Parse FHIRPath string into AST."""
        pass

    def optimize(self, ast: FHIRPathAST) -> FHIRPathAST:
        """Optimize AST for population-scale execution."""
        pass

    def generate_cte_plan(self, ast: FHIRPathAST) -> CTEPlan:
        """Generate CTE execution plan."""
        pass
```

#### **Layer 3: CTE Generator**
Converts FHIRPath expressions into CTE (Common Table Expression) chains.

```python
class CTEGenerator:
    """Generates CTE chains from FHIRPath expressions."""

    def generate(self, expression: FHIRPathAST) -> CTEChain:
        """
        Convert FHIRPath AST to CTE chain.

        Example:
        "Patient.name.given.first()" →

        WITH
          patient_names AS (
            SELECT id, json_extract(resource, '$.name') as names
            FROM fhir_resources
            WHERE resourceType = 'Patient'
          ),
          given_names AS (
            SELECT id, json_extract(names, '$[*].given') as given_array
            FROM patient_names
          ),
          first_given AS (
            SELECT id, json_extract(given_array, '$[0]') as result
            FROM given_names
          )
        SELECT id, result FROM first_given
        """
        pass
```

**CTE Generation Principles**:
1. **One CTE per FHIRPath operation** (first(), where(), combine(), etc.)
2. **Population-first**: Each CTE operates on full dataset
3. **Dependency-aware**: CTEs ordered by dependency graph
4. **Optimizable**: Database engine optimizes CTE chains
5. **Debuggable**: Each CTE can be inspected independently

#### **Layer 4: SQL Assembler**
Combines CTEs into monolithic SQL queries.

```python
class SQLAssembler:
    """Assembles CTE chains into final SQL queries."""

    def assemble(self, cte_chains: List[CTEChain]) -> MonolithicSQL:
        """
        Combines multiple CTE chains into single query.
        Perfect for CQL with multiple define statements.
        """
        pass
```

#### **Layer 5: Thin Dialect Layer**
Pure syntax translation without business logic.

```python
class SQLDialect:
    """Base class for database-specific syntax."""

    def json_extract(self, obj: str, path: str) -> str:
        """Database-specific JSON extraction."""
        raise NotImplementedError

    def json_array_agg(self, expr: str) -> str:
        """Database-specific JSON array aggregation."""
        raise NotImplementedError

    def with_clause(self, name: str, query: str) -> str:
        """Database-specific WITH clause syntax."""
        return f"{name} AS ({query})"
```

**DuckDB Dialect Example**:
```python
class DuckDBDialect(SQLDialect):
    def json_extract(self, obj: str, path: str) -> str:
        return f"json_extract({obj}, '{path}')"

    def json_array_agg(self, expr: str) -> str:
        return f"json_group_array({expr})"
```

**No business logic in dialects - only syntax differences.**

---

## CTE-First Design Pattern

### Philosophy: Every FHIRPath Function = CTE Template

Instead of complex operation chains, each FHIRPath function maps directly to a CTE template:

```python
class FHIRPathFunctions:
    """CTE templates for FHIRPath functions."""

    def first(self, input_cte: str, array_column: str) -> str:
        """first() function as CTE template."""
        return f"""
        SELECT *,
               json_extract({array_column}, '$[0]') as result
        FROM {input_cte}
        """

    def where(self, input_cte: str, condition: str) -> str:
        """where() function as CTE template."""
        return f"""
        SELECT *
        FROM {input_cte}
        WHERE {condition}
        """

    def combine(self, input_cte: str, array_column: str) -> str:
        """combine() function as CTE template."""
        return f"""
        SELECT id,
               json_group_array({array_column}) as result
        FROM {input_cte}
        GROUP BY id
        """
```

### CTE Chain Example

**FHIRPath Expression**: `Patient.name.given.where(startsWith('J')).first()`

**Generated CTE Chain**:
```sql
WITH
  -- Step 1: Extract names array
  patient_names AS (
    SELECT id, json_extract(resource, '$.name') as names
    FROM fhir_resources
    WHERE resourceType = 'Patient'
  ),

  -- Step 2: Extract given names (flattened)
  given_names AS (
    SELECT id,
           json_extract(name_item.value, '$.given') as given_array
    FROM patient_names
    CROSS JOIN json_each(names) AS name_item
  ),

  -- Step 3: Apply where() filter
  filtered_given AS (
    SELECT id, given_array
    FROM given_names
    WHERE json_extract(given_array, '$[0]') LIKE 'J%'
  ),

  -- Step 4: Apply first()
  first_given AS (
    SELECT id,
           json_extract(given_array, '$[0]') as result
    FROM filtered_given
  )

SELECT id, result FROM first_given
```

**Benefits**:
- **Readable**: Each step is clear and inspectable
- **Debuggable**: Can test each CTE individually
- **Optimizable**: Database engine optimizes the entire chain
- **Population-scale**: Operates on full dataset efficiently

---

## Population-Scale Analytics Design

### Core Principle: Population-First, Not Row-by-Row

**Traditional Approach (Row-by-Row)**:
```python
# Anti-pattern: Process one patient at a time
for patient in patients:
    result = evaluate_fhirpath(patient, expression)
    results.append(result)
```

**Population-First Approach**:
```sql
-- Process all patients in single query
WITH population_results AS (
  SELECT id,
         json_extract(resource, '$.name[0].family') as family_name
  FROM fhir_resources
  WHERE resourceType = 'Patient'
)
SELECT * FROM population_results
```

### Monolithic CQL Execution

**CQL with Multiple Defines**:
```cql
define "HasDiabetes":
  exists([Condition: "Diabetes"])

define "RecentA1c":
  [Observation: "HbA1c"] O
  where O.effective during "Measurement Period"

define "InInitialPopulation":
  "HasDiabetes" and exists("RecentA1c")
```

**Generated Monolithic SQL**:
```sql
WITH
  -- CTE for HasDiabetes define
  has_diabetes AS (
    SELECT patient_id,
           COUNT(*) > 0 as result
    FROM conditions
    WHERE code = 'diabetes_code'
    GROUP BY patient_id
  ),

  -- CTE for RecentA1c define
  recent_a1c AS (
    SELECT patient_id,
           json_group_array(resource) as observations
    FROM observations
    WHERE code = 'a1c_code'
      AND effective_date >= '2024-01-01'
    GROUP BY patient_id
  ),

  -- CTE for InInitialPopulation define
  initial_population AS (
    SELECT h.patient_id,
           h.result AND (r.observations IS NOT NULL) as result
    FROM has_diabetes h
    LEFT JOIN recent_a1c r ON h.patient_id = r.patient_id
  )

-- Final results for all defines
SELECT
  patient_id,
  has_diabetes.result as "HasDiabetes",
  recent_a1c.observations as "RecentA1c",
  initial_population.result as "InInitialPopulation"
FROM initial_population
LEFT JOIN has_diabetes USING (patient_id)
LEFT JOIN recent_a1c USING (patient_id)
```

**Benefits**:
- **Single query execution** instead of N separate queries
- **Database optimization** across entire measure
- **Massive performance gains** (10x+ improvement demonstrated)
- **Simplified debugging** - one query to inspect

---
