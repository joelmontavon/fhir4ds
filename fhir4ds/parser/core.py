from .lexer import FHIRPathLexer
from .parser import Parser

def parse(expression: str, resource: dict = None):
    """
    Parse a FHIRPath expression into an Abstract Syntax Tree (AST).

    Args:
        expression: The FHIRPath expression to parse
        resource: The FHIR resource context (optional for now)

    Returns:
        The parsed AST node representing the expression
    """
    try:
        # Use the enhanced lexer and parser
        lexer = FHIRPathLexer(expression)
        tokens = list(lexer.tokenize())
        parser = Parser(tokens)
        ast = parser.parse()
        return ast
    except Exception as e:
        print(f"Error parsing expression '{expression}': {e}")
        return None