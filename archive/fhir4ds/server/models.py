"""
FHIR4DS Server API Models

Pydantic models for API requests and responses.
"""

from typing import List, Dict, Any, Optional, Union
from datetime import datetime
from pydantic import BaseModel, Field, validator
from enum import Enum


class OutputFormat(str, Enum):
    """Supported output formats for analytics results"""
    JSON = "json"
    CSV = "csv"
    EXCEL = "excel"
    PARQUET = "parquet"


class ViewDefinitionRequest(BaseModel):
    """Request model for creating ViewDefinitions"""
    
    name: str = Field(..., description="Unique name for the ViewDefinition")
    description: Optional[str] = Field(None, description="Human-readable description")
    resource: str = Field(..., description="FHIR resource type (e.g., 'Patient', 'Observation')")
    status: Optional[str] = Field("active", description="ViewDefinition status")
    select: List[Dict[str, Any]] = Field(..., description="SELECT clause definition")
    where: Optional[List[Dict[str, Any]]] = Field(None, description="WHERE clause conditions")
    
    @validator('name')
    def validate_name(cls, v):
        """Validate ViewDefinition name"""
        if not v or not v.strip():
            raise ValueError("ViewDefinition name cannot be empty")
        if not v.replace('_', '').replace('-', '').isalnum():
            raise ValueError("ViewDefinition name can only contain letters, numbers, underscores, and hyphens")
        return v.strip()
    
    @validator('resource')
    def validate_resource(cls, v):
        """Validate FHIR resource type"""
        if not v or not v.strip():
            raise ValueError("Resource type cannot be empty")
        return v.strip()


class ViewDefinitionResponse(BaseModel):
    """Response model for ViewDefinition information"""
    
    name: str
    description: Optional[str]
    resource: str
    status: str
    created_at: datetime
    updated_at: datetime
    select: List[Dict[str, Any]]
    where: Optional[List[Dict[str, Any]]]


class AnalyticsRequest(BaseModel):
    """Request model for executing analytics"""
    
    resources: List[Dict[str, Any]] = Field(..., description="FHIR resources to analyze")
    format: Optional[OutputFormat] = Field(OutputFormat.JSON, description="Output format")
    include_metadata: Optional[bool] = Field(True, description="Include query metadata in response")
    
    @validator('resources')
    def validate_resources(cls, v):
        """Validate FHIR resources"""
        if not v:
            raise ValueError("At least one resource must be provided")
        
        for i, resource in enumerate(v):
            if not isinstance(resource, dict):
                raise ValueError(f"Resource {i} must be a dictionary")
            if 'resourceType' not in resource:
                raise ValueError(f"Resource {i} must have a 'resourceType' field")
        
        return v


class AnalyticsResponse(BaseModel):
    """Response model for analytics results"""
    
    view_name: str
    executed_at: datetime
    resource_count: int
    result_count: int
    execution_time_ms: float
    format: OutputFormat
    data: Union[List[Dict[str, Any]], str]  # JSON data or CSV string
    metadata: Optional[Dict[str, Any]] = None


class BulkResourceRequest(BaseModel):
    """Request model for bulk resource loading"""
    
    resources: List[Dict[str, Any]] = Field(..., description="FHIR resources to load")
    parallel: Optional[bool] = Field(True, description="Use parallel processing")
    batch_size: Optional[int] = Field(100, description="Batch size for processing")
    
    @validator('resources')
    def validate_resources(cls, v):
        """Validate FHIR resources"""
        if not v:
            raise ValueError("At least one resource must be provided")
        
        for i, resource in enumerate(v):
            if not isinstance(resource, dict):
                raise ValueError(f"Resource {i} must be a dictionary")
            if 'resourceType' not in resource:
                raise ValueError(f"Resource {i} must have a 'resourceType' field")
        
        return v


class BulkResourceResponse(BaseModel):
    """Response model for bulk resource loading"""
    
    loaded_count: int
    processing_time_ms: float
    parallel_processing: bool
    batch_size: int


class ServerInfo(BaseModel):
    """Server information and statistics"""
    
    name: str = "FHIR4DS Analytics Server"
    version: str = "3.0.0"
    status: str = "healthy"
    database_type: str
    database_url: str
    uptime_seconds: float
    total_views: int
    total_resources: int
    total_queries_executed: int
    features: List[str]


class ErrorResponse(BaseModel):
    """Error response model"""
    
    error: str
    detail: str
    timestamp: datetime
    request_id: Optional[str] = None


class ViewListResponse(BaseModel):
    """Response model for listing ViewDefinitions"""
    
    views: List[ViewDefinitionResponse]
    total_count: int


class HealthResponse(BaseModel):
    """Health check response"""
    
    status: str = "healthy"
    timestamp: datetime
    database_connected: bool
    version: str = "3.0.0"