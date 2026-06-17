"""
CQL Logical Operators

Implements HL7 CQL specification logical operators with proper
three-valued logic (true/false/null) handling.
"""

from typing import Optional, Union

class CQLLogicalOperators:
    """CQL Logical operator implementations."""
    
    @staticmethod
    def and_op(left: Optional[bool], right: Optional[bool]) -> Optional[bool]:
        """
        CQL And operator with three-valued logic.
        
        Truth table:
        T and T = T    T and F = F    T and null = null
        F and T = F    F and F = F    F and null = F
        null and T = null    null and F = F    null and null = null
        """
        if left is False or right is False:
            return False
        if left is None or right is None:
            return None
        return left and right
    
    @staticmethod
    def or_op(left: Optional[bool], right: Optional[bool]) -> Optional[bool]:
        """
        CQL Or operator with three-valued logic.
        
        Truth table:
        T or T = T    T or F = T    T or null = T
        F or T = T    F or F = F    F or null = null
        null or T = T    null or F = null    null or null = null
        """
        if left is True or right is True:
            return True
        if left is None or right is None:
            return None
        return left or right
    
    @staticmethod
    def not_op(value: Optional[bool]) -> Optional[bool]:
        """
        CQL Not operator with three-valued logic.
        
        Truth table:
        not T = F
        not F = T
        not null = null
        """
        if value is None:
            return None
        return not value
    
    @staticmethod
    def implies_op(left: Optional[bool], right: Optional[bool]) -> Optional[bool]:
        """
        CQL Implies operator with three-valued logic.
        
        Truth table:
        T implies T = T    T implies F = F    T implies null = null
        F implies T = T    F implies F = T    F implies null = T
        null implies T = T    null implies F = null    null implies null = null
        """
        if left is False:
            return True
        if left is None:
            return None if right is None else (True if right is True else None)
        return right
    
    @staticmethod
    def xor_op(left: Optional[bool], right: Optional[bool]) -> Optional[bool]:
        """
        CQL Xor operator with three-valued logic.
        
        Truth table:
        T xor T = F    T xor F = T    T xor null = null
        F xor T = T    F xor F = F    F xor null = null
        null xor T = null    null xor F = null    null xor null = null
        """
        if left is None or right is None:
            return None
        return left != right
    
    def get_supported_functions(self):
        """Return list of supported logical functions."""
        return ['and', 'or', 'not', 'implies', 'xor']
