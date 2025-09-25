"""
FHIR Library Resource Handler

This module provides functionality to work with FHIR Library resources containing CQL content.
Supports base64 encoding/decoding of CQL content as per FHIR specification.
"""

import base64
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)


class FHIRLibraryHandler:
    """
    Handler for FHIR Library resources containing CQL content.
    
    This class provides methods to:
    - Extract CQL content from FHIR Library resources
    - Create FHIR Library resources with base64-encoded CQL
    - Validate Library resource structure
    """
    
    def __init__(self):
        """Initialize the FHIR Library Handler."""
        self.supported_content_types = [
            'text/cql',
            'application/elm+xml',
            'application/elm+json'
        ]
    
    def extract_cql_from_library(self, library_resource: Dict[str, Any]) -> str:
        """
        Extract and decode base64 CQL content from FHIR Library resource.
        
        Args:
            library_resource: FHIR Library resource as dictionary
            
        Returns:
            Decoded CQL content as string
            
        Raises:
            ValueError: If Library resource is invalid or CQL content not found
        """
        if not isinstance(library_resource, dict):
            raise ValueError("Library resource must be a dictionary")
        
        if library_resource.get('resourceType') != 'Library':
            raise ValueError(f"Expected Library resource, got {library_resource.get('resourceType')}")
        
        # Get the content array
        content_list = library_resource.get('content', [])
        if not content_list:
            raise ValueError("Library resource has no content")
        
        # Find CQL content
        cql_content = None
        for content_item in content_list:
            content_type = content_item.get('contentType', '')
            
            if content_type in ['text/cql', 'text/cql-expression']:
                # Found CQL content
                if 'data' not in content_item:
                    raise ValueError("CQL content item has no data field")
                
                # Decode base64 content
                try:
                    encoded_data = content_item['data']
                    decoded_bytes = base64.b64decode(encoded_data)
                    cql_content = decoded_bytes.decode('utf-8')
                    break
                except Exception as e:
                    logger.error(f"Failed to decode base64 CQL content: {e}")
                    raise ValueError(f"Invalid base64 CQL content: {e}")
        
        if cql_content is None:
            available_types = [item.get('contentType') for item in content_list]
            raise ValueError(f"No CQL content found. Available content types: {available_types}")
        
        logger.info(f"Successfully extracted CQL content ({len(cql_content)} characters)")
        return cql_content
    
    def create_library_resource(self, 
                              cql_content: str, 
                              library_id: str,
                              name: Optional[str] = None,
                              version: Optional[str] = None,
                              title: Optional[str] = None,
                              description: Optional[str] = None) -> Dict[str, Any]:
        """
        Create FHIR Library resource with base64-encoded CQL content.
        
        Args:
            cql_content: CQL content as string
            library_id: Unique identifier for the library
            name: Human-readable name (defaults to library_id)
            version: Library version (defaults to "1.0.0")
            title: Library title
            description: Library description
            
        Returns:
            FHIR Library resource as dictionary
        """
        if not cql_content or not isinstance(cql_content, str):
            raise ValueError("CQL content must be a non-empty string")
        
        if not library_id or not isinstance(library_id, str):
            raise ValueError("Library ID must be a non-empty string")
        
        # Encode CQL content to base64
        try:
            encoded_cql = base64.b64encode(cql_content.encode('utf-8')).decode('utf-8')
        except Exception as e:
            raise ValueError(f"Failed to encode CQL content: {e}")
        
        # Create Library resource
        library_resource = {
            "resourceType": "Library",
            "id": library_id,
            "meta": {
                "profile": [
                    "http://hl7.org/fhir/uv/cpg/StructureDefinition/cpg-executablelibrary"
                ]
            },
            "identifier": [
                {
                    "use": "official",
                    "value": library_id
                }
            ],
            "name": name or library_id,
            "version": version or "1.0.0",
            "status": "active",
            "experimental": False,
            "type": {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/library-type",
                        "code": "logic-library",
                        "display": "Logic Library"
                    }
                ]
            },
            "date": datetime.now().isoformat(),
            "content": [
                {
                    "contentType": "text/cql",
                    "data": encoded_cql
                }
            ]
        }
        
        # Add optional fields
        if title:
            library_resource["title"] = title
            
        if description:
            library_resource["description"] = description
        
        logger.info(f"Created Library resource '{library_id}' with {len(cql_content)} characters of CQL")
        return library_resource
    
    def validate_library_resource(self, library_resource: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate FHIR Library resource structure.
        
        Args:
            library_resource: FHIR Library resource to validate
            
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
        if not isinstance(library_resource, dict):
            validation_result["valid"] = False
            validation_result["errors"].append("Library resource must be a dictionary")
            return validation_result
        
        # Check resource type
        if library_resource.get('resourceType') != 'Library':
            validation_result["valid"] = False
            validation_result["errors"].append(f"Expected resourceType 'Library', got '{library_resource.get('resourceType')}'")
        
        # Check required fields
        required_fields = ['id', 'status', 'type', 'content']
        for field in required_fields:
            if field not in library_resource:
                validation_result["errors"].append(f"Missing required field: {field}")
                validation_result["valid"] = False
        
        # Check content array
        content_list = library_resource.get('content', [])
        if not content_list:
            validation_result["errors"].append("Library must have at least one content item")
            validation_result["valid"] = False
        else:
            # Check for CQL content
            has_cql = False
            for content_item in content_list:
                content_type = content_item.get('contentType', '')
                if content_type in ['text/cql', 'text/cql-expression']:
                    has_cql = True
                    if 'data' not in content_item:
                        validation_result["errors"].append("CQL content item missing 'data' field")
                        validation_result["valid"] = False
                    break
            
            if not has_cql:
                validation_result["warnings"].append("No CQL content found in Library")
        
        # Collect info
        validation_result["info"] = {
            "library_id": library_resource.get('id'),
            "name": library_resource.get('name'),
            "version": library_resource.get('version'),
            "status": library_resource.get('status'),
            "content_count": len(content_list)
        }
        
        return validation_result
    
    def list_library_content_types(self, library_resource: Dict[str, Any]) -> List[str]:
        """
        List all content types available in a Library resource.
        
        Args:
            library_resource: FHIR Library resource
            
        Returns:
            List of content types
        """
        content_list = library_resource.get('content', [])
        return [item.get('contentType', 'unknown') for item in content_list]
    
    def get_library_metadata(self, library_resource: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract metadata from FHIR Library resource.
        
        Args:
            library_resource: FHIR Library resource
            
        Returns:
            Dictionary with library metadata
        """
        return {
            "id": library_resource.get('id'),
            "name": library_resource.get('name'),
            "title": library_resource.get('title'),
            "version": library_resource.get('version'),
            "status": library_resource.get('status'),
            "description": library_resource.get('description'),
            "date": library_resource.get('date'),
            "content_types": self.list_library_content_types(library_resource),
            "experimental": library_resource.get('experimental', False)
        }