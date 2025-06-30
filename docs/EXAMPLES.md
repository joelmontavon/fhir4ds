# SQL-on-FHIR View Runner Examples

This document provides comprehensive examples of using the SQL-on-FHIR View Runner library.

## Basic Setup

```python
import duckdb
import json
from sql_on_fhir import SQLOnFHIRViewRunner

# Initialize runner with in-memory database
runner = SQLOnFHIRViewRunner()

# Or use existing DuckDB connection
conn = duckdb.connect("fhir_data.db")
runner = SQLOnFHIRViewRunner(connection=conn)
```

## Sample Data Setup

```python
def setup_sample_data(runner):
    """Create sample FHIR data for examples"""
    
    # Create table
    runner.connection.execute("""
        CREATE OR REPLACE TABLE fhir_resources (
            id STRING,
            resource JSON
        )
    """)
    
    # Sample patients
    patients = [
        {
            "resourceType": "Patient",
            "id": "patient1",
            "active": True,
            "name": [
                {
                    "use": "official",
                    "family": "Smith",
                    "given": ["John", "William"]
                },
                {
                    "use": "maiden",
                    "family": "Johnson",
                    "given": ["John"]
                }
            ],
            "telecom": [
                {"system": "phone", "value": "555-1234"},
                {"system": "email", "value": "john.smith@example.com"}
            ],
            "birthDate": "1990-01-01",
            "address": [{
                "use": "home",
                "line": ["123 Main St"],
                "city": "Anytown",
                "postalCode": "12345"
            }],
            "managingOrganization": {
                "reference": "Organization/org1"
            }
        },
        {
            "resourceType": "Patient", 
            "id": "patient2",
            "active": False,
            "name": [{
                "use": "official",
                "family": "Johnson",
                "given": ["Jane"]
            }],
            "birthDate": "1985-05-15"
        }
    ]
    
    # Sample organizations
    organizations = [
        {
            "resourceType": "Organization",
            "id": "org1",
            "name": "General Hospital",
            "type": [{
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/organization-type",
                    "code": "prov",
                    "display": "Healthcare Provider"
                }]
            }]
        }
    ]
    
    # Insert sample data
    for patient in patients:
        runner.connection.execute(
            "INSERT INTO fhir_resources VALUES (?, ?)",
            [patient['id'], json.dumps(patient)]
        )
    
    for org in organizations:
        runner.connection.execute(
            "INSERT INTO fhir_resources VALUES (?, ?)",
            [org['id'], json.dumps(org)]
        )

# Setup data for examples
setup_sample_data(runner)
```

## Example 1: Basic Patient Demographics

```python
# Simple patient demographics view
patient_demographics = {
    "name": "patient_demographics",
    "resource": "Patient",
    "select": [{
        "column": [
            {
                "name": "patient_id",
                "path": "getResourceKey()",
                "type": "id"
            },
            {
                "name": "family_name",
                "path": "name.where(use='official').family.first()",
                "type": "string"
            },
            {
                "name": "given_name",
                "path": "name.where(use='official').given.first()",
                "type": "string"
            },
            {
                "name": "birth_date",
                "path": "birthDate",
                "type": "date"
            },
            {
                "name": "is_active",
                "path": "active",
                "type": "boolean"
            }
        ]
    }]
}

results = runner.execute_view_definition(patient_demographics)
print("Patient Demographics:")
for row in results.fetchall():
    print(f"  {row}")
```

## Example 2: Using WHERE Clauses

```python
# Active patients only
active_patients = {
    "name": "active_patients",
    "resource": "Patient",
    "where": [
        {"path": "active = true"}
    ],
    "select": [{
        "column": [
            {
                "name": "id",
                "path": "getResourceKey()",
                "type": "id"
            },
            {
                "name": "name",
                "path": "name.where(use='official').family.first()",
                "type": "string"
            }
        ]
    }]
}

results = runner.execute_view_definition(active_patients)
print("\\nActive Patients:")
for row in results.fetchall():
    print(f"  {row}")
```

## Example 3: Collection Handling

```python
# Patient contact information with collections
patient_contacts = {
    "name": "patient_contacts",
    "resource": "Patient",
    "select": [{
        "column": [
            {
                "name": "patient_id",
                "path": "getResourceKey()",
                "type": "id"
            },
            {
                "name": "all_given_names",
                "path": "name.given",
                "type": "string",
                "collection": True  # Returns array of all given names
            },
            {
                "name": "phone_numbers",
                "path": "telecom.where(system='phone').value",
                "type": "string",
                "collection": True
            },
            {
                "name": "email_addresses", 
                "path": "telecom.where(system='email').value",
                "type": "string",
                "collection": True
            }
        ]
    }]
}

results = runner.execute_view_definition(patient_contacts)
print("\\nPatient Contacts:")
for row in results.fetchall():
    print(f"  {row}")
```

## Example 4: forEach Operations

```python
# Flatten patient names using forEach
patient_names_flattened = {
    "name": "patient_names_flattened",
    "resource": "Patient",
    "select": [{
        "forEach": "name",
        "column": [
            {
                "name": "patient_id",
                "path": "getResourceKey()",
                "type": "id"
            },
            {
                "name": "name_use",
                "path": "use",
                "type": "code"
            },
            {
                "name": "family",
                "path": "family",
                "type": "string"
            },
            {
                "name": "given",
                "path": "given.first()",
                "type": "string"
            }
        ]
    }]
}

results = runner.execute_view_definition(patient_names_flattened)
print("\\nFlattened Patient Names:")
for row in results.fetchall():
    print(f"  {row}")
```

## Example 5: Reference Handling

```python
# Patient with organization references
patient_organizations = {
    "name": "patient_organizations",
    "resource": "Patient", 
    "select": [{
        "column": [
            {
                "name": "patient_id",
                "path": "getResourceKey()",
                "type": "id"
            },
            {
                "name": "patient_name",
                "path": "name.where(use='official').family.first()",
                "type": "string"
            },
            {
                "name": "organization_ref",
                "path": "managingOrganization.reference",
                "type": "string"
            },
            {
                "name": "organization_id",
                "path": "managingOrganization.getReferenceKey(Organization)",
                "type": "id"
            }
        ]
    }]
}

results = runner.execute_view_definition(patient_organizations)
print("\\nPatient Organizations:")
for row in results.fetchall():
    print(f"  {row}")
```

## Example 6: Constants and Template Variables

```python
# Using constants in ViewDefinition
patient_status_view = {
    "name": "patient_status",
    "resource": "Patient",
    "constant": [
        {
            "name": "active_status",
            "valueBoolean": True
        },
        {
            "name": "status_label",
            "valueString": "Active Patient"
        }
    ],
    "where": [
        {"path": "active = %active_status"}
    ],
    "select": [{
        "column": [
            {
                "name": "patient_id",
                "path": "getResourceKey()",
                "type": "id"
            },
            {
                "name": "name",
                "path": "name.family.first()",
                "type": "string"
            },
            {
                "name": "status",
                "path": "%status_label",
                "type": "string"
            }
        ]
    }]
}

results = runner.execute_view_definition(patient_status_view)
print("\\nPatient Status with Constants:")
for row in results.fetchall():
    print(f"  {row}")
```

## Example 7: Complex Nested Structures

```python
# Complex view with nested selects and unions
patient_addresses = {
    "name": "patient_addresses",
    "resource": "Patient",
    "select": [
        {
            # Patient basic info
            "column": [
                {
                    "name": "patient_id",
                    "path": "getResourceKey()",
                    "type": "id"
                },
                {
                    "name": "patient_name",
                    "path": "name.family.first()",
                    "type": "string"
                }
            ]
        },
        {
            # Address details using forEach
            "forEach": "address",
            "column": [
                {
                    "name": "address_use",
                    "path": "use",
                    "type": "code"
                },
                {
                    "name": "address_line",
                    "path": "line.first()",
                    "type": "string"
                },
                {
                    "name": "city",
                    "path": "city",
                    "type": "string"
                },
                {
                    "name": "postal_code",
                    "path": "postalCode",
                    "type": "string"
                }
            ]
        }
    ]
}

results = runner.execute_view_definition(patient_addresses)
print("\\nPatient Addresses:")
for row in results.fetchall():
    print(f"  {row}")
```

## Example 8: Error Handling and Validation

```python
# Example showing collection constraint validation
try:
    # This should fail because family name can be multiple values
    # but collection is set to false
    invalid_view = {
        "name": "invalid_collection",
        "resource": "Patient",
        "select": [{
            "column": [{
                "name": "family_names",
                "path": "name.family",  # This returns multiple values
                "type": "string",
                "collection": False  # But collection is false
            }]
        }]
    }
    
    results = runner.execute_view_definition(invalid_view)
    print("This should not execute successfully")
    
except ValueError as e:
    if str(e) == "Collection value":
        print("\\nCollection validation working correctly!")
        print("Error: Cannot return multiple values when collection=false")
```

## Example 9: Creating Database Views

```python
# Create a materialized view for performance
view_sql = runner.create_view(patient_demographics, materialized=True)
print(f"\\nCreated materialized view with SQL:\\n{view_sql}")

# Query the created view directly
direct_results = runner.connection.execute("SELECT * FROM patient_demographics").fetchall()
print("\\nDirect query of materialized view:")
for row in direct_results:
    print(f"  {row}")
```

## Example 10: Schema Introspection

```python
# Get schema information for a view
schema = runner.get_schema(patient_demographics)
print("\\nView Schema:")
for col in schema:
    print(f"  Column: {col['name']}")
    print(f"    FHIR Type: {col['fhir_type']}")
    print(f"    Collection: {col['collection']}")
    print(f"    FHIRPath: {col['path']}")
    print()
```

## Example 11: Working with Extensions

```python
# Sample patient with extensions
patient_with_extensions = {
    "resourceType": "Patient",
    "id": "patient3",
    "extension": [
        {
            "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
            "valueCodeableConcept": {
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/v3-Race",
                    "code": "2106-3",
                    "display": "White"
                }]
            }
        },
        {
            "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity", 
            "valueCodeableConcept": {
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/v3-Ethnicity",
                    "code": "2186-5",
                    "display": "Not Hispanic or Latino"
                }]
            }
        }
    ],
    "name": [{
        "family": "Johnson",
        "given": ["Michael"]
    }]
}

# Insert the patient with extensions
runner.connection.execute(
    "INSERT INTO fhir_resources VALUES (?, ?)",
    [patient_with_extensions['id'], json.dumps(patient_with_extensions)]
)

# View definition using extension function
patient_race_ethnicity = {
    "name": "patient_demographics_extended",
    "resource": "Patient",
    "select": [{
        "column": [
            {
                "name": "patient_id",
                "path": "getResourceKey()",
                "type": "id"
            },
            {
                "name": "name",
                "path": "name.family.first()",
                "type": "string"
            },
            {
                "name": "race_code",
                "path": "extension('http://hl7.org/fhir/us/core/StructureDefinition/us-core-race').valueCodeableConcept.coding.code.first()",
                "type": "code"
            },
            {
                "name": "ethnicity_code",
                "path": "extension('http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity').valueCodeableConcept.coding.code.first()",
                "type": "code"
            }
        ]
    }]
}

results = runner.execute_view_definition(patient_race_ethnicity)
print("\\nPatient Race and Ethnicity:")
for row in results.fetchall():
    print(f"  {row}")
```

## Performance Tips

### 1. Use Materialized Views for Complex Queries

```python
# For frequently accessed complex views, create materialized views
complex_view = {
    "name": "complex_patient_summary",
    "resource": "Patient",
    "select": [{
        "forEach": "name",
        "column": [
            # Many complex columns...
        ]
    }]
}

# Create materialized view for better performance
runner.create_view(complex_view, materialized=True)
```

### 2. Index Your FHIR Resources

```python
# Create indexes on frequently queried fields
runner.connection.execute("""
    CREATE INDEX idx_resource_type 
    ON fhir_resources (json_extract_string(resource, '$.resourceType'))
""")

runner.connection.execute("""
    CREATE INDEX idx_patient_active 
    ON fhir_resources (json_extract(resource, '$.active'))
    WHERE json_extract_string(resource, '$.resourceType') = 'Patient'
""")
```

### 3. Use Specific Resource Type Filters

```python
# Always specify resource type in WHERE clause for better performance
optimized_view = {
    "name": "optimized_patients",
    "resource": "Patient",  # This automatically adds resource type filter
    "select": [{"column": [...]}]
}
```

## Error Scenarios and Debugging

### 1. Collection Constraint Violations

```python
# Debug collection issues
try:
    problematic_view = {
        "name": "debug_collections",
        "resource": "Patient", 
        "select": [{
            "column": [{
                "name": "names",
                "path": "name.family",  # Multiple values possible
                "collection": False  # But collection=false
            }]
        }]
    }
    runner.execute_view_definition(problematic_view)
except ValueError as e:
    print(f"Collection error: {e}")
```

### 2. Invalid FHIRPath Expressions

```python
# Handle FHIRPath syntax errors
try:
    invalid_path_view = {
        "name": "invalid_path",
        "resource": "Patient",
        "select": [{
            "column": [{
                "name": "invalid",
                "path": "invalid..syntax",  # Invalid FHIRPath
                "type": "string"
            }]
        }]
    }
    runner.execute_view_definition(invalid_path_view)
except Exception as e:
    print(f"FHIRPath error: {e}")
```

### 3. Missing Resources

```python
# Handle missing resource types
try:
    missing_resource_view = {
        "name": "missing_resource",
        "resource": "NonExistentResource",
        "select": [{"column": [{"name": "id", "path": "id"}]}]
    }
    results = runner.execute_view_definition(missing_resource_view)
    print(f"Results for missing resource: {results.fetchall()}")  # Will be empty
except Exception as e:
    print(f"Resource error: {e}")
```

## Advanced Usage

### Custom Database Configuration

```python
# Use custom table and column names
custom_runner = SQLOnFHIRViewRunner(
    table_name="custom_fhir_data",
    json_column="fhir_resource_json"
)

# Set up custom table structure
custom_runner.connection.execute("""
    CREATE TABLE custom_fhir_data (
        resource_id STRING,
        fhir_resource_json JSON,
        created_date DATE
    )
""")
```

### Batch Processing Multiple Views

```python
# Process multiple views efficiently
views_to_process = [
    patient_demographics,
    active_patients,
    patient_contacts
]

results = {}
for view_def in views_to_process:
    view_name = view_def['name']
    try:
        result = runner.execute_view_definition(view_def)
        results[view_name] = result.fetchall()
        print(f"✅ Processed {view_name}: {len(results[view_name])} rows")
    except Exception as e:
        print(f"❌ Failed {view_name}: {e}")
        results[view_name] = None
```

This comprehensive set of examples demonstrates the full capabilities of the SQL-on-FHIR View Runner library, from basic usage to advanced scenarios and error handling.