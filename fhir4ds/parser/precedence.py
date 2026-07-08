# fhir4ds.parser.precedence

"""
Operator precedence and associativity for the FHIRPath parser.

This table is based on the official FHIRPath R4 specification.
http://hl7.org/fhir/fhirpath.html#operator-precedence
"""

# Precedence levels for binary operators, from lowest to highest.
# Unary operators, function calls, indexing, and member access are handled
# by the parser's recursive descent structure, not by this precedence table.
PRECEDENCE = {
    # Logical OR
    'or': 10,

    # Logical AND
    'and': 20,

    # Membership
    'in': 30,
    'contains': 30,

    # Equality
    '=': 40,
    '!=': 40,
    '~': 40,
    '!~': 40,

    # Relational
    '<': 50,
    '<=': 50,
    '>': 50,
    '>=': 50,

    # Additive
    '+': 60,
    '-': 60,

    # Multiplicative
    '*': 70,
    '/': 70,
    'mod': 70,
}

# Per the FHIRPath spec, most binary operators are left-associative.
ASSOCIATIVITY = {op: 'LEFT' for op in PRECEDENCE}
