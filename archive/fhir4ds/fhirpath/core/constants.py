"""
Constants and utility definitions for SQL-on-FHIR processing.
"""

# FHIR primitive types that are typically stored as strings
FHIR_PRIMITIVE_TYPES_AS_STRING = [
    "string", "uri", "url", "canonical", "code", "id", "markdown",
    "oid", "uuid", "base64binary", "date", "datetime", "instant", "time"
]

# Common FHIR resource types (subset - can be expanded)
FHIR_RESOURCE_TYPES = [
    "Patient", "Practitioner", "Organization", "Location", "Device",
    "Observation", "Condition", "Procedure", "MedicationRequest", 
    "DiagnosticReport", "Encounter", "Appointment", "AllergyIntolerance",
    "CarePlan", "CareTeam", "Goal", "Immunization", "Media"
]

# Boolean fields commonly found in FHIR resources
KNOWN_BOOLEAN_FIELDS = ['active', 'deceasedBoolean']

# SQL operators mapping for FHIRPath
SQL_OPERATORS = {
    'and': 'AND',
    'or': 'OR', 
    '=': '=',
    '!=': '!=',
    '~': 'IS NOT DISTINCT FROM',      # Equivalence operator - treats null as equal to null
    '!~': 'IS DISTINCT FROM',         # Not equivalence operator - treats null as not equal to null
    '|': 'UNION ALL',                 # Collection union operator
    '>': '>',
    '<': '<',
    '>=': '>=',
    '<=': '<=',
    '+': '+',
    '-': '-',
    '*': '*',
    '/': '/',
    'div': 'DIV',      # Integer division operator
    'mod': 'MOD',      # Modulo/remainder operator
    'xor': 'XOR',      # Exclusive OR operator (Phase 6 Week 15)
    'implies': 'IMPLIES'  # Logical implication operator (Phase 6 Week 15)
}