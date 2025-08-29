"""
FHIR Parameters Resource Handler

This module provides functionality to work with FHIR Parameters resources for CQL library invocation.
Supports parameter extraction, validation, and creation as per FHIR specification.
"""

import logging
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, date

logger = logging.getLogger(__name__)


class ParametersHandler:
    """
    Handler for FHIR Parameters resources for CQL library invocation.
    
    This class provides methods to:
    - Extract parameters from FHIR Parameters resources
    - Create FHIR Parameters resources for library invocation
    - Validate Parameters resource structure
    """
    
    def __init__(self):
        """Initialize the FHIR Parameters Handler."""
        self.supported_parameter_types = [
            'string', 'integer', 'decimal', 'boolean', 'date', 'dateTime',
            'time', 'code', 'uri', 'id', 'Reference'
        ]
    
    def extract_parameters(self, parameters_resource: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract parameters from FHIR Parameters resource.
        
        Args:
            parameters_resource: FHIR Parameters resource as dictionary
            
        Returns:
            Dictionary with parameter names as keys and values as values
            
        Raises:
            ValueError: If Parameters resource is invalid
        """
        if not isinstance(parameters_resource, dict):
            raise ValueError("Parameters resource must be a dictionary")
        
        if parameters_resource.get('resourceType') != 'Parameters':
            raise ValueError(f"Expected Parameters resource, got {parameters_resource.get('resourceType')}")
        
        # Extract parameters from parameter array
        parameter_list = parameters_resource.get('parameter', [])
        extracted_params = {}
        
        for param_item in parameter_list:
            param_name = param_item.get('name')
            if not param_name:
                logger.warning("Parameter item missing 'name' field, skipping")
                continue
            
            # Extract value based on type
            param_value = self._extract_parameter_value(param_item)
            extracted_params[param_name] = param_value
        
        logger.info(f"Extracted {len(extracted_params)} parameters from Parameters resource")
        return extracted_params
    
    def _extract_parameter_value(self, param_item: Dict[str, Any]) -> Any:
        """
        Extract the value from a parameter item based on its type.
        
        Args:
            param_item: Individual parameter item from Parameters.parameter array
            
        Returns:
            The parameter value in appropriate Python type
        """
        # Check for different value types
        value_fields = [
            'valueString', 'valueInteger', 'valueDecimal', 'valueBoolean',
            'valueDate', 'valueDateTime', 'valueTime', 'valueCode', 
            'valueUri', 'valueId', 'valueReference'
        ]
        
        for value_field in value_fields:
            if value_field in param_item:
                value = param_item[value_field]
                
                # Convert to appropriate Python type
                if value_field == 'valueInteger':
                    return int(value)
                elif value_field == 'valueDecimal':
                    return float(value)
                elif value_field == 'valueBoolean':
                    return bool(value)
                elif value_field in ['valueDate', 'valueDateTime']:
                    # Keep as string for now, could parse to datetime if needed
                    return value
                elif value_field == 'valueReference':
                    # Return reference as string (e.g., "Library/example-library")
                    return value.get('reference', str(value))
                else:
                    # String types
                    return str(value)
        
        # No value found
        logger.warning(f"No value found in parameter item: {param_item}")
        return None
    
    def create_parameters_resource(self, 
                                 resource_id: str,
                                 library_reference: Optional[str] = None,
                                 **parameters) -> Dict[str, Any]:
        """
        Create FHIR Parameters resource for library invocation.
        
        Args:
            resource_id: Unique identifier for the Parameters resource
            library_reference: Reference to the Library resource (e.g., "Library/example-lib")
            **parameters: Parameter name-value pairs
            
        Returns:
            FHIR Parameters resource as dictionary
        """
        if not resource_id or not isinstance(resource_id, str):
            raise ValueError("Resource ID must be a non-empty string")
        
        # Create Parameters resource structure
        parameters_resource = {
            "resourceType": "Parameters",
            "id": resource_id,
            "meta": {
                "profile": [
                    "http://hl7.org/fhir/StructureDefinition/Parameters"
                ]
            },
            "parameter": []
        }
        
        # Add library reference if provided
        if library_reference:
            parameters_resource["parameter"].append({
                "name": "library",
                "valueReference": {
                    "reference": library_reference
                }
            })
        
        # Add custom parameters
        for param_name, param_value in parameters.items():
            param_item = self._create_parameter_item(param_name, param_value)
            if param_item:
                parameters_resource["parameter"].append(param_item)
        
        logger.info(f"Created Parameters resource '{resource_id}' with {len(parameters_resource['parameter'])} parameters")
        return parameters_resource
    
    def _create_parameter_item(self, name: str, value: Any) -> Optional[Dict[str, Any]]:
        """
        Create a parameter item for the Parameters resource.
        
        Args:
            name: Parameter name
            value: Parameter value
            
        Returns:
            Parameter item dictionary or None if value type not supported
        """
        if not name:
            return None
        
        param_item = {"name": name}
        
        # Determine value type and create appropriate field
        if isinstance(value, str):
            param_item["valueString"] = value
        elif isinstance(value, int):
            param_item["valueInteger"] = value
        elif isinstance(value, float):
            param_item["valueDecimal"] = value
        elif isinstance(value, bool):
            param_item["valueBoolean"] = value
        elif isinstance(value, (date, datetime)):
            if isinstance(value, datetime):
                param_item["valueDateTime"] = value.isoformat()
            else:
                param_item["valueDate"] = value.isoformat()
        elif isinstance(value, dict) and 'reference' in value:
            # Reference type
            param_item["valueReference"] = value
        else:
            # Try to convert to string
            try:
                param_item["valueString"] = str(value)
            except:
                logger.warning(f"Unsupported parameter type for '{name}': {type(value)}")
                return None
        
        return param_item
    
    def validate_parameters_resource(self, parameters_resource: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate FHIR Parameters resource structure.
        
        Args:
            parameters_resource: FHIR Parameters resource to validate
            
        Returns:
            Dictionary with validation results
        """
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "info": {}
        }
        
        # Check basic structure
        if not isinstance(parameters_resource, dict):
            validation_result["valid"] = False
            validation_result["errors"].append("Parameters resource must be a dictionary")
            return validation_result
        
        # Check resource type
        if parameters_resource.get('resourceType') != 'Parameters':
            validation_result["valid"] = False
            validation_result["errors"].append(f"Expected resourceType 'Parameters', got '{parameters_resource.get('resourceType')}'")
        
        # Check parameter array
        parameter_list = parameters_resource.get('parameter', [])
        if not isinstance(parameter_list, list):
            validation_result["errors"].append("'parameter' field must be an array")
            validation_result["valid"] = False
        else:
            # Validate individual parameters
            for i, param_item in enumerate(parameter_list):
                if not isinstance(param_item, dict):
                    validation_result["errors"].append(f"Parameter item {i} must be a dictionary")
                    validation_result["valid"] = False
                    continue
                
                if 'name' not in param_item:
                    validation_result["errors"].append(f"Parameter item {i} missing 'name' field")
                    validation_result["valid"] = False
                
                # Check for at least one value field
                has_value = any(key.startswith('value') for key in param_item.keys())
                if not has_value:
                    validation_result["warnings"].append(f"Parameter '{param_item.get('name', i)}' has no value field")
        
        # Collect info
        validation_result["info"] = {
            "resource_id": parameters_resource.get('id'),
            "parameter_count": len(parameter_list),
            "parameter_names": [p.get('name') for p in parameter_list if isinstance(p, dict)]
        }
        
        return validation_result
    
    def get_library_reference(self, parameters_resource: Dict[str, Any]) -> Optional[str]:
        """
        Extract library reference from Parameters resource.
        
        Args:
            parameters_resource: FHIR Parameters resource
            
        Returns:
            Library reference string or None if not found
        """
        parameter_list = parameters_resource.get('parameter', [])
        
        for param_item in parameter_list:
            if param_item.get('name') == 'library':
                if 'valueReference' in param_item:
                    return param_item['valueReference'].get('reference')
                elif 'valueString' in param_item:
                    return param_item['valueString']
        
        return None
    
    def list_parameter_names(self, parameters_resource: Dict[str, Any]) -> List[str]:
        """
        List all parameter names in a Parameters resource.
        
        Args:
            parameters_resource: FHIR Parameters resource
            
        Returns:
            List of parameter names
        """
        parameter_list = parameters_resource.get('parameter', [])
        return [param_item.get('name') for param_item in parameter_list 
                if isinstance(param_item, dict) and 'name' in param_item]
    
    def get_parameters_metadata(self, parameters_resource: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract metadata from FHIR Parameters resource.
        
        Args:
            parameters_resource: FHIR Parameters resource
            
        Returns:
            Dictionary with parameters metadata
        """
        return {
            "id": parameters_resource.get('id'),
            "parameter_count": len(parameters_resource.get('parameter', [])),
            "parameter_names": self.list_parameter_names(parameters_resource),
            "library_reference": self.get_library_reference(parameters_resource)
        }