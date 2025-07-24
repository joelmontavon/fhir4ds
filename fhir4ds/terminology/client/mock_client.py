"""
Mock Terminology Client

Provides a mock terminology client for testing and development purposes.
Returns predictable responses without making external API calls.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from .base_client import BaseTerminologyClient, TerminologyServiceError

logger = logging.getLogger(__name__)


class MockTerminologyClient(BaseTerminologyClient):
    """
    Mock terminology client for testing purposes.
    
    Returns predictable responses without external API calls,
    useful for testing, development, and demonstration.
    """
    
    def __init__(self, name: str = "Mock Terminology Server", 
                 simulate_errors: bool = False):
        """
        Initialize mock terminology client.
        
        Args:
            name: Name of the mock service
            simulate_errors: Whether to simulate occasional errors
        """
        self.name = name
        self.simulate_errors = simulate_errors
        self.version = "1.0.0"
        self.call_count = 0
        
        # Predefined test data
        self.test_valuesets = {
            'http://example.org/fhir/ValueSet/test-conditions': {
                'codes': [
                    {'code': '386661006', 'display': 'Fever', 'system': 'http://snomed.info/sct'},
                    {'code': '25064002', 'display': 'Headache', 'system': 'http://snomed.info/sct'},
                    {'code': '422400008', 'display': 'Vomiting', 'system': 'http://snomed.info/sct'}
                ]
            },
            'http://example.org/fhir/ValueSet/test-medications': {
                'codes': [
                    {'code': '387207008', 'display': 'Aspirin', 'system': 'http://snomed.info/sct'},
                    {'code': '387362001', 'display': 'Ibuprofen', 'system': 'http://snomed.info/sct'}
                ]
            }
        }
        
        self.test_codes = {
            'http://snomed.info/sct': {
                '386661006': {'display': 'Fever', 'definition': 'Elevated body temperature'},
                '25064002': {'display': 'Headache', 'definition': 'Pain in the head'},
                '387207008': {'display': 'Aspirin', 'definition': 'Acetylsalicylic acid'},
                '387362001': {'display': 'Ibuprofen', 'definition': 'Nonsteroidal anti-inflammatory drug'}
            },
            'http://loinc.org': {
                '8310-5': {'display': 'Body temperature', 'definition': 'Measurement of body temperature'},
                '8480-6': {'display': 'Systolic blood pressure', 'definition': 'Systolic blood pressure measurement'}
            }
        }
        
        logger.info(f"Initialized mock terminology client: {name}")
    
    def _increment_call_count(self) -> None:
        """Increment call counter and optionally simulate errors."""
        self.call_count += 1
        
        if self.simulate_errors and self.call_count % 10 == 0:
            raise TerminologyServiceError(
                "Simulated error for testing purposes",
                status_code=500
            )
    
    def expand_valueset(self, valueset_url: str, version: str = None,
                       parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Mock expand ValueSet operation.
        
        Returns predefined test data for known ValueSets.
        """
        self._increment_call_count()
        logger.debug(f"Mock expanding ValueSet: {valueset_url}")
        
        if valueset_url in self.test_valuesets:
            codes = self.test_valuesets[valueset_url]['codes']
            
            expansion = {
                'resourceType': 'ValueSet',
                'id': 'mock-expansion',
                'url': valueset_url,
                'version': version or '1.0.0',
                'name': 'MockValueSet',
                'title': f'Mock ValueSet for {valueset_url}',
                'status': 'active',
                'date': datetime.now().isoformat(),
                'expansion': {
                    'identifier': f'mock-expansion-{self.call_count}',
                    'timestamp': datetime.now().isoformat(),
                    'total': len(codes),
                    'contains': codes
                }
            }
        else:
            # Return empty expansion for unknown ValueSets
            expansion = {
                'resourceType': 'ValueSet',
                'id': 'mock-expansion-empty',
                'url': valueset_url,
                'version': version or '1.0.0',
                'name': 'MockEmptyValueSet',
                'title': f'Mock Empty ValueSet for {valueset_url}',
                'status': 'active',
                'date': datetime.now().isoformat(),
                'expansion': {
                    'identifier': f'mock-expansion-empty-{self.call_count}',
                    'timestamp': datetime.now().isoformat(),
                    'total': 0,
                    'contains': []
                }
            }
        
        logger.debug(f"Mock expansion returned {expansion['expansion']['total']} codes")
        return expansion
    
    def validate_code(self, code: str, system: str, valueset_url: str = None,
                     display: str = None) -> Dict[str, Any]:
        """
        Mock validate code operation.
        
        Returns true for known test codes, false otherwise.
        """
        self._increment_call_count()
        logger.debug(f"Mock validating code: {code} in system {system}")
        
        # Check if code exists in test data
        is_valid = (system in self.test_codes and 
                   code in self.test_codes[system])
        
        # If validating against a specific ValueSet, check that too
        if is_valid and valueset_url and valueset_url in self.test_valuesets:
            valueset_codes = [c['code'] for c in self.test_valuesets[valueset_url]['codes']]
            is_valid = code in valueset_codes
        
        result = {
            'resourceType': 'Parameters',
            'parameter': [
                {
                    'name': 'result',
                    'valueBoolean': is_valid
                }
            ]
        }
        
        if is_valid and system in self.test_codes and code in self.test_codes[system]:
            result['parameter'].append({
                'name': 'display',
                'valueString': self.test_codes[system][code]['display']
            })
        
        logger.debug(f"Mock validation result: {is_valid}")
        return result
    
    def lookup_code(self, code: str, system: str,
                   properties: List[str] = None) -> Dict[str, Any]:
        """
        Mock lookup code operation.
        
        Returns predefined details for known test codes.
        """
        self._increment_call_count()
        logger.debug(f"Mock looking up code: {code} in system {system}")
        
        if system in self.test_codes and code in self.test_codes[system]:
            code_info = self.test_codes[system][code]
            
            parameters = [
                {'name': 'name', 'valueString': system},
                {'name': 'version', 'valueString': '1.0.0'},
                {'name': 'display', 'valueString': code_info['display']},
                {'name': 'definition', 'valueString': code_info['definition']}
            ]
            
            # Add requested properties
            if properties:
                for prop in properties:
                    if prop in code_info:
                        parameters.append({
                            'name': 'property',
                            'part': [
                                {'name': 'code', 'valueString': prop},
                                {'name': 'value', 'valueString': str(code_info[prop])}
                            ]
                        })
        else:
            # Code not found
            parameters = [
                {'name': 'name', 'valueString': system},
                {'name': 'version', 'valueString': '1.0.0'}
            ]
        
        result = {
            'resourceType': 'Parameters',
            'parameter': parameters
        }
        
        logger.debug(f"Mock lookup returned {len(parameters)} parameters")
        return result
    
    def subsumes(self, code_a: str, code_b: str, system: str) -> Dict[str, Any]:
        """
        Mock subsumption test.
        
        Returns simple subsumption logic for testing.
        """
        self._increment_call_count()
        logger.debug(f"Mock testing subsumption: {code_a} subsumes {code_b}")
        
        # Simple mock logic: fever (386661006) subsumes headache (25064002)
        subsumes_result = (
            system == 'http://snomed.info/sct' and 
            code_a == '386661006' and 
            code_b == '25064002'
        )
        
        outcome = 'subsumes' if subsumes_result else 'not-subsumed'
        
        result = {
            'resourceType': 'Parameters',
            'parameter': [
                {
                    'name': 'outcome',
                    'valueString': outcome
                }
            ]
        }
        
        logger.debug(f"Mock subsumption result: {outcome}")
        return result
    
    def get_supported_operations(self) -> List[str]:
        """Get list of supported mock operations."""
        return ['expand', 'validate-code', 'lookup', 'subsumes']
    
    def get_service_info(self) -> Dict[str, Any]:
        """Get mock service information."""
        return {
            'name': self.name,
            'client_version': self.version,
            'type': 'mock',
            'operations': self.get_supported_operations(),
            'test_valuesets': len(self.test_valuesets),
            'test_codes': sum(len(codes) for codes in self.test_codes.values()),
            'total_calls': self.call_count,
            'simulate_errors': self.simulate_errors
        }
    
    def add_test_valueset(self, url: str, codes: List[Dict[str, str]]) -> None:
        """
        Add a test ValueSet to the mock client.
        
        Args:
            url: ValueSet URL
            codes: List of codes with 'code', 'display', 'system' keys
        """
        self.test_valuesets[url] = {'codes': codes}
        logger.info(f"Added test ValueSet: {url} with {len(codes)} codes")
    
    def add_test_code(self, system: str, code: str, display: str, 
                     definition: str = None) -> None:
        """
        Add a test code to the mock client.
        
        Args:
            system: Code system URL
            code: Code value
            display: Display name
            definition: Code definition (optional)
        """
        if system not in self.test_codes:
            self.test_codes[system] = {}
        
        self.test_codes[system][code] = {
            'display': display,
            'definition': definition or f'Mock definition for {code}'
        }
        
        logger.info(f"Added test code: {code} in {system}")
    
    def reset_call_count(self) -> None:
        """Reset the call counter."""
        self.call_count = 0
        logger.info("Reset mock client call counter")
    
    def get_call_statistics(self) -> Dict[str, Any]:
        """Get statistics about mock client usage."""
        return {
            'total_calls': self.call_count,
            'available_valuesets': list(self.test_valuesets.keys()),
            'available_systems': list(self.test_codes.keys()),
            'total_test_codes': sum(len(codes) for codes in self.test_codes.values())
        }