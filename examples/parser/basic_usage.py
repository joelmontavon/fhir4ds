#!/usr/bin/env python3
"""
Basic FHIRPath Parser Usage Examples

This script demonstrates the basic usage of the FHIR4DS FHIRPath parser,
including parsing simple expressions, handling errors, and working with AST nodes.
"""

from fhir4ds.parser import parse
from fhir4ds.parser.lexer import FHIRPathLexer
from fhir4ds.parser.parser import Parser
from fhir4ds.parser.exceptions import ParseError, LexerError
from fhir4ds.ast.visitors import ASTPrinter


def basic_parsing_examples():
    """Demonstrate basic parsing functionality."""
    print("=== Basic Parsing Examples ===\n")

    # Example 1: Simple property access
    print("1. Simple property access:")
    expression = "Patient.name"
    ast = parse(expression)
    print(f"Expression: {expression}")
    print(f"AST type: {type(ast).__name__}")
    print()

    # Example 2: Function call
    print("2. Function call:")
    expression = "Patient.name.first()"
    ast = parse(expression)
    print(f"Expression: {expression}")
    print(f"AST type: {type(ast).__name__}")
    print()

    # Example 3: Complex expression
    print("3. Complex expression:")
    expression = "Patient.telecom.where(system = 'phone').value"
    ast = parse(expression)
    print(f"Expression: {expression}")
    print(f"AST type: {type(ast).__name__}")
    print()


def step_by_step_parsing():
    """Demonstrate step-by-step parsing process."""
    print("=== Step-by-Step Parsing ===\n")

    expression = "Patient.name.given"
    print(f"Parsing expression: {expression}")

    # Step 1: Lexical analysis
    print("\nStep 1: Lexical Analysis")
    lexer = FHIRPathLexer(expression)
    tokens = list(lexer.tokenize())

    for i, token in enumerate(tokens):
        if token.token_type.name != 'EOF':  # Skip EOF token for readability
            print(f"  Token {i}: {token.token_type.name} = '{token.value}'")

    # Step 2: Parsing
    print("\nStep 2: Parsing")
    parser = Parser(tokens)
    ast = parser.parse()
    print(f"  Root AST node: {type(ast).__name__}")
    print()


def ast_visualization():
    """Demonstrate AST visualization."""
    print("=== AST Visualization ===\n")

    expressions = [
        "Patient.name",
        "Patient.name.given.first()",
        "age > 18"
    ]

    printer = ASTPrinter()

    for expr in expressions:
        print(f"Expression: {expr}")
        try:
            ast = parse(expr)
            print("AST Structure:")
            print(printer.visit(ast))
        except (ParseError, LexerError) as e:
            print(f"Error: {e}")
        print("-" * 40)


def error_handling_examples():
    """Demonstrate error handling."""
    print("=== Error Handling Examples ===\n")

    invalid_expressions = [
        "Patient.name.",      # Incomplete expression
        "Patient.'invalid",   # Unterminated string
        "Patient name",       # Missing dot
        "123abc",            # Invalid identifier
    ]

    for expr in invalid_expressions:
        print(f"Testing invalid expression: '{expr}'")
        try:
            ast = parse(expr)
            print(f"  Unexpected success: {type(ast).__name__}")
        except ParseError as e:
            print(f"  ParseError: {e}")
        except LexerError as e:
            print(f"  LexerError: {e}")
        except Exception as e:
            print(f"  Other error: {type(e).__name__}: {e}")
        print()


def token_type_examples():
    """Demonstrate different token types."""
    print("=== Token Type Examples ===\n")

    expressions = [
        "Patient",                    # Identifier
        "'John Doe'",                # String literal
        "42",                        # Integer literal
        "3.14",                      # Decimal literal
        "true",                      # Boolean literal
        "@2023-01-01",               # Date literal
        "@T12:30:00",                # Time literal
    ]

    for expr in expressions:
        print(f"Expression: {expr}")
        try:
            lexer = FHIRPathLexer(expr)
            tokens = list(lexer.tokenize())
            # Show non-EOF tokens
            relevant_tokens = [t for t in tokens if t.token_type.name != 'EOF']
            for token in relevant_tokens:
                print(f"  {token.token_type.name}: '{token.value}'")
        except LexerError as e:
            print(f"  Error: {e}")
        print()


if __name__ == "__main__":
    """Run all examples."""
    print("FHIRPath Parser Basic Usage Examples")
    print("=" * 50)
    print()

    try:
        basic_parsing_examples()
        step_by_step_parsing()
        ast_visualization()
        error_handling_examples()
        token_type_examples()

        print("All examples completed successfully!")

    except ImportError as e:
        print(f"Import error: {e}")
        print("Make sure the FHIR4DS package is properly installed.")
    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()