from .lexer import FHIRPathLexer
from .parser import Parser
from .exceptions import ParseError, LexerError

def parse(expression: str, resource: dict = None):
    """
    Parse a FHIRPath expression into an Abstract Syntax Tree (AST).

    Args:
        expression: The FHIRPath expression to parse
        resource: The FHIR resource context (optional for now)

    Returns:
        The parsed AST node representing the expression.

    Raises:
        LexerError: If the expression cannot be tokenized.
        ParseError: If the expression is syntactically incorrect.
    """
    # The try/except block was removed to allow exceptions to bubble up to the caller,
    # which is necessary for the test suite to correctly identify and handle parsing failures.
    lexer = FHIRPathLexer(expression)
    tokens = list(lexer.tokenize())
    parser = Parser(tokens)
    ast = parser.parse()
    return ast