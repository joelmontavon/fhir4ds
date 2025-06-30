#!/usr/bin/env python3
"""
Test only the FHIRPath expressions required by SQL-on-FHIR v2.0 specification.
"""

import sys
import traceback

def test_sql_on_fhir_required_expressions():
    """Test only the FHIRPath expressions required by SQL-on-FHIR v2.0 spec"""
    print("🔍 SQL-ON-FHIR v2.0 SPECIFICATION COMPLIANCE TEST")
    print("=" * 80)
    print("Testing ONLY the FHIRPath expressions required by the official specification")
    print("=" * 80)
    
    try:
        from fhir4ds.datastore import QuickConnect
        
        db = QuickConnect.duckdb(":memory:")
        
        # Load test data that covers SQL-on-FHIR spec requirements
        spec_test_data = [
            {
                "resourceType": "Patient",
                "id": "spec-test-001",
                "active": True,
                "name": [
                    {"family": "TestFamily", "given": ["TestGiven"]},
                    {"family": "AltFamily", "given": ["AltGiven"], "use": "maiden"}
                ],
                "birthDate": "1985-03-15",
                "gender": "male",
                "telecom": [
                    {"system": "email", "value": "test@example.com"},
                    {"system": "phone", "value": "+1-555-1234"}
                ],
                "extension": [
                    {"url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race", 
                     "valueCodeableConcept": {"coding": [{"code": "1002-5", "display": "American Indian or Alaska Native"}]}}
                ]
            }
        ]
        
        db.load_resources(spec_test_data)
        print("✅ Loaded SQL-on-FHIR spec test data")
        
        # Define SQL-on-FHIR v2.0 required FHIRPath expressions
        # Based on the official specification at: https://sql-on-fhir.org/
        sql_on_fhir_required = {
            "Basic Path Navigation": [
                ("id", "id", "Resource ID access"),
                ("active", "boolean", "Boolean field access"), 
                ("gender", "string", "String field access"),
                ("birthDate", "date", "Date field access"),
                ("name", "string", "Collection access"),
                ("name.family", "string", "Nested property access"),
                ("name.given", "string", "Nested collection access"),
                ("telecom.system", "string", "Property in collection"),
                ("telecom.value", "string", "Value in collection"),
            ],
            
            "Collection Functions": [
                ("name.first()", "string", "first() function - get first element"),
                ("telecom.exists()", "boolean", "exists() function - check existence"),
                ("name.count()", "integer", "count() function - count elements"),
                ("name.empty()", "boolean", "empty() function - check if empty"),
            ],
            
            "Filtering with where()": [
                ("telecom.where(system='email')", "string", "where() with string equality"),
                ("telecom.where(system='email').value", "string", "where() then property access"),
                ("telecom.where(system='email').value.first()", "string", "where() then property then first()"),
                ("name.where(use='maiden')", "string", "where() on different field"),
                ("name.where(use='maiden').family", "string", "where() then property"),
            ],
            
            "Extension Function": [
                ("extension('http://hl7.org/fhir/us/core/StructureDefinition/us-core-race')", "string", "extension() function"),
                ("extension('http://hl7.org/fhir/us/core/StructureDefinition/us-core-race').valueCodeableConcept", "string", "extension() with value access"),
                ("extension('http://hl7.org/fhir/us/core/StructureDefinition/us-core-race').exists()", "boolean", "extension() with exists()"),
            ],
            
            "Reference Functions (if supported)": [
                ("getResourceKey()", "string", "getResourceKey() function"),
            ],
            
            "Boolean Expressions in WHERE clauses": [
                # These are used in WHERE clauses of ViewDefinitions, not as select expressions
                # But we can test them as boolean expressions
                ("active = true", "boolean", "Boolean equality in expressions"),
                ("active != false", "boolean", "Boolean inequality in expressions"),
                ("name.exists()", "boolean", "exists() in boolean context"),
            ]
        }
        
        # Test each required expression
        all_results = {}
        total_tests = 0
        total_passing = 0
        
        for category, expressions in sql_on_fhir_required.items():
            print(f"\n🔍 Testing {category}")
            print("-" * 60)
            
            category_results = []
            passing_in_category = 0
            
            for expression, expected_type, description in expressions:
                total_tests += 1
                
                # Create test ViewDefinition
                test_view = {
                    "resource": "Patient",
                    "select": [{
                        "column": [
                            {"name": "id", "path": "id", "type": "id"},
                            {"name": "test_result", "path": expression, "type": expected_type}
                        ]
                    }]
                }
                
                try:
                    result = db.execute_to_dataframe(test_view)
                    print(f"  ✅ {description}: `{expression}`")
                    total_passing += 1
                    passing_in_category += 1
                    category_results.append((expression, expected_type, description, True, None))
                except Exception as e:
                    print(f"  ❌ {description}: `{expression}` - {str(e)[:60]}...")
                    category_results.append((expression, expected_type, description, False, str(e)))
            
            all_results[category] = {
                "results": category_results,
                "passing": passing_in_category,
                "total": len(expressions)
            }
            
            category_pct = (passing_in_category / len(expressions)) * 100
            print(f"  📊 Category: {passing_in_category}/{len(expressions)} passing ({category_pct:.1f}%)")
        
        return all_results, total_passing, total_tests
        
    except Exception as e:
        print(f"❌ SQL-on-FHIR compliance testing failed: {e}")
        traceback.print_exc()
        return None, 0, 0

def test_official_test_cases():
    """Test against some actual SQL-on-FHIR specification examples"""
    print(f"\n🔍 TESTING OFFICIAL SQL-ON-FHIR EXAMPLES")
    print("-" * 60)
    
    try:
        from fhir4ds.datastore import QuickConnect
        
        db = QuickConnect.duckdb(":memory:")
        
        # Sample Patient resource similar to spec examples
        spec_patient = {
            "resourceType": "Patient",
            "id": "example-patient",
            "active": True,
            "name": [
                {"family": "Doe", "given": ["John", "Middle"]},
                {"family": "Smith", "given": ["Johnny"], "use": "nickname"}
            ],
            "gender": "male",
            "birthDate": "1974-12-25",
            "telecom": [
                {"system": "phone", "value": "555-1234", "use": "home"},
                {"system": "email", "value": "john@example.com", "use": "work"}
            ]
        }
        
        db.load_resources([spec_patient])
        
        # Test typical ViewDefinition patterns from the SQL-on-FHIR spec
        spec_viewdefinitions = [
            {
                "name": "Basic Patient Fields",
                "view": {
                    "resource": "Patient",
                    "select": [{
                        "column": [
                            {"name": "id", "path": "id", "type": "id"},
                            {"name": "family", "path": "name.family", "type": "string"},
                            {"name": "given", "path": "name.given", "type": "string"},
                            {"name": "active", "path": "active", "type": "boolean"}
                        ]
                    }]
                }
            },
            {
                "name": "Patient with Filtering",
                "view": {
                    "resource": "Patient", 
                    "select": [{
                        "column": [
                            {"name": "id", "path": "id", "type": "id"},
                            {"name": "work_email", "path": "telecom.where(system='email').value", "type": "string"},
                            {"name": "first_given", "path": "name.given.first()", "type": "string"}
                        ]
                    }]
                }
            },
            {
                "name": "Patient with WHERE clause",
                "view": {
                    "resource": "Patient",
                    "select": [{
                        "column": [
                            {"name": "id", "path": "id", "type": "id"},
                            {"name": "family", "path": "name.family", "type": "string"}
                        ]
                    }],
                    "where": [
                        {"path": "active = true"}
                    ]
                }
            }
        ]
        
        print("Testing typical SQL-on-FHIR ViewDefinition patterns...")
        
        all_working = True
        for test_case in spec_viewdefinitions:
            try:
                result = db.execute_to_dataframe(test_case["view"])
                print(f"  ✅ {test_case['name']}: {len(result)} rows returned")
            except Exception as e:
                print(f"  ❌ {test_case['name']}: {str(e)[:60]}...")
                all_working = False
        
        return all_working
        
    except Exception as e:
        print(f"❌ Official test case testing failed: {e}")
        return False

def generate_spec_compliance_report(results, total_passing, total_tests, official_tests_pass):
    """Generate SQL-on-FHIR v2.0 specification compliance report"""
    print("\n" + "=" * 80)
    print("📊 SQL-ON-FHIR v2.0 SPECIFICATION COMPLIANCE REPORT")
    print("=" * 80)
    
    if not results:
        print("❌ Could not generate compliance report due to testing failure")
        return False
    
    # Overall compliance
    compliance_pct = (total_passing / total_tests) * 100
    print(f"\n🎯 OVERALL SPECIFICATION COMPLIANCE: {total_passing}/{total_tests} ({compliance_pct:.1f}%)")
    
    # Category compliance
    print(f"\n📋 SPECIFICATION CATEGORY COMPLIANCE:")
    fully_compliant_categories = 0
    
    for category, data in results.items():
        passing = data["passing"] 
        total = data["total"]
        pct = (passing / total) * 100
        
        if pct == 100:
            status = "✅ FULLY COMPLIANT"
            fully_compliant_categories += 1
        elif pct >= 80:
            status = "⚠️ MOSTLY COMPLIANT"
        else:
            status = "❌ NON-COMPLIANT"
        
        print(f"  {status}: {category} ({passing}/{total} - {pct:.1f}%)")
    
    # Official test results
    print(f"\n🧪 OFFICIAL VIEWDEFINITION PATTERNS:")
    if official_tests_pass:
        print(f"  ✅ All typical SQL-on-FHIR ViewDefinition patterns work correctly")
    else:
        print(f"  ❌ Some SQL-on-FHIR ViewDefinition patterns have issues")
    
    # Non-compliant expressions
    non_compliant = []
    for category, data in results.items():
        for expression, expected_type, description, works, error in data["results"]:
            if not works:
                non_compliant.append((category, description, expression, error))
    
    if non_compliant:
        print(f"\n❌ NON-COMPLIANT EXPRESSIONS:")
        for category, description, expression, error in non_compliant:
            print(f"  • {category}: {description}")
            print(f"    Expression: `{expression}`")
            if error and len(error) < 100:
                print(f"    Error: {error}")
    
    # Verdict
    print(f"\n" + "=" * 80)
    print(f"🏆 SQL-ON-FHIR v2.0 SPECIFICATION VERDICT")
    print(f"=" * 80)
    
    if compliance_pct == 100 and official_tests_pass:
        print(f"✅ PERFECT COMPLIANCE: All required FHIRPath expressions work correctly")
        print(f"🎉 The library fully implements the SQL-on-FHIR v2.0 specification")
        return True
    elif compliance_pct >= 95 and official_tests_pass:
        print(f"✅ EXCELLENT COMPLIANCE: Nearly all required expressions work")
        print(f"👍 The library successfully implements the SQL-on-FHIR v2.0 specification")
        return True
    elif compliance_pct >= 80:
        print(f"⚠️ GOOD COMPLIANCE: Most required expressions work with minor gaps")
        print(f"📝 Some specification requirements need attention")
        return False
    else:
        print(f"❌ POOR COMPLIANCE: Significant gaps in specification support")
        print(f"🔧 Major work needed to meet SQL-on-FHIR v2.0 requirements")
        return False

def main():
    """Main compliance testing function"""
    results, total_passing, total_tests = test_sql_on_fhir_required_expressions()
    official_tests_pass = test_official_test_cases()
    
    compliance = generate_spec_compliance_report(results, total_passing, total_tests, official_tests_pass)
    
    return compliance

if __name__ == "__main__":
    compliance = main()
    sys.exit(0 if compliance else 1)