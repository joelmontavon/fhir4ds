"""
Base Terminology Client Interface

Defines the standard interface for terminology service clients,
making the system extensible to multiple terminology services.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List


class BaseTerminologyClient(ABC):
    """
    Base class for terminology service clients.
    
    All terminology service implementations should inherit from this class
    to ensure consistent interfaces for CQL functionality.
    """
    
    @abstractmethod
    def expand_valueset(self, valueset_url: str, version: str = None, 
                       parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Expand valueset to get all codes.
        
        Args:
            valueset_url: ValueSet canonical URL or identifier
            version: Specific version (optional)
            parameters: Additional parameters for expansion
            
        Returns:
            FHIR ValueSet resource with expansion
            
        Raises:
            TerminologyServiceError: If expansion fails
        """
        pass
    
    @abstractmethod
    def validate_code(self, code: str, system: str, valueset_url: str = None,
                     display: str = None) -> Dict[str, Any]:
        """
        Validate if code exists in system/valueset.
        
        Args:
            code: Code to validate
            system: Code system URL
            valueset_url: ValueSet URL (optional)
            display: Display name to validate (optional)
            
        Returns:
            Validation result with boolean result and details
            
        Raises:
            TerminologyServiceError: If validation fails
        """
        pass
    
    @abstractmethod
    def lookup_code(self, code: str, system: str, 
                   properties: List[str] = None) -> Dict[str, Any]:
        """
        Get code details from code system.
        
        Args:
            code: Code to lookup
            system: Code system URL
            properties: Properties to return (optional)
            
        Returns:
            FHIR Parameters resource with code details
            
        Raises:
            TerminologyServiceError: If lookup fails
        """
        pass
    
    @abstractmethod
    def subsumes(self, code_a: str, code_b: str, system: str) -> Dict[str, Any]:
        """
        Test if code_a subsumes code_b.
        
        Args:
            code_a: First code (potential parent)
            code_b: Second code (potential child)
            system: Code system URL
            
        Returns:
            Subsumption result with boolean outcome
            
        Raises:
            TerminologyServiceError: If subsumption test fails
        """
        pass
    
    def get_supported_operations(self) -> List[str]:
        """
        Get list of supported operations.
        
        Returns:
            List of operation names supported by this client
        """
        return ['expand', 'validate-code', 'lookup', 'subsumes']
    
    def get_service_info(self) -> Dict[str, Any]:
        """
        Get information about the terminology service.
        
        Returns:
            Service information including name, version, capabilities
        """
        return {
            'name': self.__class__.__name__,
            'operations': self.get_supported_operations(),
            'version': getattr(self, 'version', 'unknown')
        }


class TerminologyServiceError(Exception):
    """Exception raised when terminology service operations fail."""
    
    def __init__(self, message: str, status_code: int = None, 
                 response_data: Dict[str, Any] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data or {}