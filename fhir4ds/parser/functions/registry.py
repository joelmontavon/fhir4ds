from typing import Dict, List, Optional
from fhir4ds.parser.functions.base import FHIRPathFunction


class FHIRPathFunctionRegistry:
    """Registry for FHIRPath function implementations"""

    def __init__(self):
        self._functions: Dict[str, FHIRPathFunction] = {}

    def register_function(self, function: FHIRPathFunction):
        """Register a new FHIRPath function"""
        self._functions[function.name()] = function

    def get_function(self, name: str) -> Optional[FHIRPathFunction]:
        """Get function implementation by name"""
        return self._functions.get(name)

    def list_functions(self) -> List[str]:
        """List all registered function names"""
        return list(self._functions.keys())