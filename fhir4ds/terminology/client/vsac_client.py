"""
VSAC (Value Set Authority Center) Terminology Client

Implements the VSAC FHIR Terminology Service API for accessing
clinical terminology and value sets.

API Documentation: https://www.nlm.nih.gov/vsac/support/usingvsac/vsacfhirapi.html
"""

import requests
import logging
from typing import Dict, Any, Optional, List
from urllib.parse import urljoin

from .base_client import BaseTerminologyClient, TerminologyServiceError

logger = logging.getLogger(__name__)


class VSACClient(BaseTerminologyClient):
    """
    VSAC FHIR Terminology Service client.
    
    Provides access to VSAC value sets and code systems using the FHIR
    terminology service API with UMLS API key authentication.
    """
    
    def __init__(self, api_key: str, base_url: str = "https://cts.nlm.nih.gov/fhir/",
                 timeout: int = 30, verify_ssl: bool = True):
        """
        Initialize VSAC client.
        
        Args:
            api_key: UMLS API key for authentication
            base_url: VSAC FHIR API base URL
            timeout: Request timeout in seconds
            verify_ssl: Whether to verify SSL certificates
        """
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.version = "FHIR 4.0.1"
        
        # Set up session with authentication
        self.session = requests.Session()
        self.session.auth = ('apikey', api_key)  # Basic auth with API key as password
        self.session.verify = verify_ssl
        self.session.headers.update({
            'Accept': 'application/fhir+json',
            'User-Agent': 'FHIR4DS-CQL-Engine/1.0'
        })
        
        logger.info(f"Initialized VSAC client with base URL: {self.base_url}")
    
    def expand_valueset(self, valueset_url: str, version: str = None, 
                       parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Expand VSAC valueset using $expand operation.
        
        Args:
            valueset_url: ValueSet canonical URL or VSAC OID
            version: Specific version (optional)
            parameters: Additional parameters (count, offset, etc.)
            
        Returns:
            FHIR ValueSet resource with expansion
            
        Raises:
            TerminologyServiceError: If expansion fails
        """
        logger.debug(f"Expanding valueset: {valueset_url}")
        
        endpoint = f"{self.base_url}/ValueSet/$expand"
        params = {'url': valueset_url}
        
        if version:
            params['version'] = version
            
        # Add additional parameters
        if parameters:
            params.update(parameters)
        
        try:
            response = self.session.get(endpoint, params=params, timeout=self.timeout)
            
            if response.status_code == 200:
                result = response.json()
                logger.debug(f"Successfully expanded valueset {valueset_url}")
                return result
            else:
                error_msg = f"VSAC expansion failed: {response.status_code}"
                if response.text:
                    error_msg += f" - {response.text[:200]}"
                raise TerminologyServiceError(error_msg, response.status_code, 
                                            self._parse_error_response(response))
                
        except requests.RequestException as e:
            logger.error(f"VSAC request failed for {valueset_url}: {e}")
            raise TerminologyServiceError(f"VSAC request failed: {e}")
    
    def validate_code(self, code: str, system: str, valueset_url: str = None,
                     display: str = None) -> Dict[str, Any]:
        """
        Validate code using VSAC $validate-code operation.
        
        Args:
            code: Code to validate
            system: Code system URL
            valueset_url: ValueSet URL (optional)
            display: Display name to validate (optional)
            
        Returns:
            FHIR Parameters resource with validation result
        """
        logger.debug(f"Validating code {code} in system {system}")
        
        endpoint = f"{self.base_url}/ValueSet/$validate-code"
        params = {'code': code, 'system': system}
        
        if valueset_url:
            params['url'] = valueset_url
        if display:
            params['display'] = display
        
        try:
            response = self.session.get(endpoint, params=params, timeout=self.timeout)
            
            if response.status_code == 200:
                result = response.json()
                logger.debug(f"Code validation completed for {code}")
                return result
            else:
                error_msg = f"VSAC validation failed: {response.status_code}"
                raise TerminologyServiceError(error_msg, response.status_code,
                                            self._parse_error_response(response))
                
        except requests.RequestException as e:
            logger.error(f"VSAC validation request failed for {code}: {e}")
            raise TerminologyServiceError(f"VSAC validation request failed: {e}")
    
    def lookup_code(self, code: str, system: str, 
                   properties: List[str] = None) -> Dict[str, Any]:
        """
        Lookup code details using VSAC $lookup operation.
        
        Args:
            code: Code to lookup
            system: Code system URL
            properties: Properties to return (optional)
            
        Returns:
            FHIR Parameters resource with code details
        """
        logger.debug(f"Looking up code {code} in system {system}")
        
        endpoint = f"{self.base_url}/CodeSystem/$lookup"
        params = {'code': code, 'system': system}
        
        if properties:
            params['property'] = properties
        
        try:
            response = self.session.get(endpoint, params=params, timeout=self.timeout)
            
            if response.status_code == 200:
                result = response.json()
                logger.debug(f"Code lookup completed for {code}")
                return result
            else:
                error_msg = f"VSAC lookup failed: {response.status_code}"
                raise TerminologyServiceError(error_msg, response.status_code,
                                            self._parse_error_response(response))
                
        except requests.RequestException as e:
            logger.error(f"VSAC lookup request failed for {code}: {e}")
            raise TerminologyServiceError(f"VSAC lookup request failed: {e}")
    
    def subsumes(self, code_a: str, code_b: str, system: str) -> Dict[str, Any]:
        """
        Test if code_a subsumes code_b using VSAC $subsumes operation.
        
        Args:
            code_a: First code (potential parent)
            code_b: Second code (potential child)
            system: Code system URL
            
        Returns:
            FHIR Parameters resource with subsumption result
        """
        logger.debug(f"Testing subsumption: {code_a} subsumes {code_b} in {system}")
        
        endpoint = f"{self.base_url}/CodeSystem/$subsumes"
        params = {
            'codeA': code_a,
            'codeB': code_b,
            'system': system
        }
        
        try:
            response = self.session.get(endpoint, params=params, timeout=self.timeout)
            
            if response.status_code == 200:
                result = response.json()
                logger.debug(f"Subsumption test completed: {code_a} vs {code_b}")
                return result
            else:
                error_msg = f"VSAC subsumption failed: {response.status_code}"
                raise TerminologyServiceError(error_msg, response.status_code,
                                            self._parse_error_response(response))
                
        except requests.RequestException as e:
            logger.error(f"VSAC subsumption request failed: {e}")
            raise TerminologyServiceError(f"VSAC subsumption request failed: {e}")
    
    def get_capability_statement(self) -> Dict[str, Any]:
        """
        Get VSAC FHIR capability statement.
        
        Returns:
            FHIR CapabilityStatement resource
        """
        logger.debug("Fetching VSAC capability statement")
        
        endpoint = f"{self.base_url}/metadata"
        
        try:
            response = self.session.get(endpoint, timeout=self.timeout)
            
            if response.status_code == 200:
                return response.json()
            else:
                error_msg = f"VSAC capability statement failed: {response.status_code}"
                raise TerminologyServiceError(error_msg, response.status_code)
                
        except requests.RequestException as e:
            logger.error(f"VSAC capability statement request failed: {e}")
            raise TerminologyServiceError(f"VSAC capability statement failed: {e}")
    
    def test_connection(self) -> bool:
        """
        Test connection to VSAC service.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.get_capability_statement()
            logger.info("VSAC connection test successful")
            return True
        except TerminologyServiceError:
            logger.warning("VSAC connection test failed")
            return False
    
    def _parse_error_response(self, response: requests.Response) -> Dict[str, Any]:
        """
        Parse error response from VSAC.
        
        Args:
            response: HTTP response object
            
        Returns:
            Parsed error information
        """
        try:
            if response.headers.get('content-type', '').startswith('application/'):
                return response.json()
        except:
            pass
        
        return {
            'status_code': response.status_code,
            'text': response.text[:500] if response.text else None
        }
    
    def get_service_info(self) -> Dict[str, Any]:
        """Get VSAC service information."""
        return {
            'name': 'VSAC (Value Set Authority Center)',
            'base_url': self.base_url,
            'version': self.version,
            'operations': self.get_supported_operations(),
            'authentication': 'UMLS API Key'
        }