"""
FHIR4DS Server Configuration

Handles server configuration from environment variables, files, and command line arguments.
"""

import os
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from pydantic import BaseSettings, Field


class FHIRAnalyticsServerConfig(BaseSettings):
    """FHIR Analytics Server Configuration"""
    
    # Server settings
    host: str = Field(default="0.0.0.0", env="FHIR4DS_HOST")
    port: int = Field(default=8000, env="FHIR4DS_PORT")
    workers: int = Field(default=1, env="FHIR4DS_WORKERS")
    reload: bool = Field(default=False, env="FHIR4DS_RELOAD")
    
    # Database settings
    database_url: str = Field(default="./fhir4ds_server.db", env="FHIR4DS_DATABASE_URL")
    database_type: str = Field(default="duckdb", env="FHIR4DS_DATABASE_TYPE")  # duckdb or postgresql
    initialize_db: bool = Field(default=True, env="FHIR4DS_INITIALIZE_DB")
    
    # ViewDefinition settings
    predefined_views_file: Optional[str] = Field(default=None, env="FHIR4DS_PREDEFINED_VIEWS")
    views_table_name: str = Field(default="fhir4ds_views", env="FHIR4DS_VIEWS_TABLE")
    
    # Server behavior
    max_resources_per_request: int = Field(default=1000, env="FHIR4DS_MAX_RESOURCES")
    default_output_format: str = Field(default="json", env="FHIR4DS_DEFAULT_FORMAT")
    enable_cors: bool = Field(default=True, env="FHIR4DS_ENABLE_CORS")
    
    # Security
    api_key: Optional[str] = Field(default=None, env="FHIR4DS_API_KEY")
    rate_limit: int = Field(default=100, env="FHIR4DS_RATE_LIMIT")  # requests per minute
    
    # Performance
    enable_parallel_processing: bool = Field(default=True, env="FHIR4DS_PARALLEL_PROCESSING")
    batch_size: int = Field(default=100, env="FHIR4DS_BATCH_SIZE")
    
    # Logging
    log_level: str = Field(default="INFO", env="FHIR4DS_LOG_LEVEL")
    log_file: Optional[str] = Field(default=None, env="FHIR4DS_LOG_FILE")
    
    class Config:
        env_prefix = "FHIR4DS_"
        case_sensitive = False


def load_predefined_views(file_path: str) -> List[Dict[str, Any]]:
    """
    Load predefined ViewDefinitions from a JSON file.
    
    Args:
        file_path: Path to JSON file containing ViewDefinitions
        
    Returns:
        List of ViewDefinition dictionaries
        
    Example file format:
    {
      "views": [
        {
          "name": "patient_demographics",
          "resource": "Patient",
          "select": [{"column": [...]}]
        }
      ]
    }
    """
    if not file_path or not Path(file_path).exists():
        return []
    
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and 'views' in data:
            return data['views']
        else:
            raise ValueError("Invalid predefined views file format")
            
    except Exception as e:
        raise ValueError(f"Failed to load predefined views from {file_path}: {e}")


def get_database_connection_string(config: ServerConfig) -> str:
    """
    Get the appropriate database connection string based on configuration.
    
    Args:
        config: Server configuration
        
    Returns:
        Database connection string
    """
    if config.database_type.lower() == "postgresql":
        # If database_url looks like a PostgreSQL connection string, use it directly
        if config.database_url.startswith(('postgresql://', 'postgres://')):
            return config.database_url
        else:
            # Default PostgreSQL connection
            return "postgresql://postgres:postgres@localhost:5432/fhir4ds"
    
    else:  # DuckDB (default)
        if config.database_url.startswith(('postgresql://', 'postgres://')):
            raise ValueError("PostgreSQL connection string provided but database_type is not 'postgresql'")
        return config.database_url


def create_default_config_file(file_path: str = "fhir4ds_server_config.json"):
    """
    Create a default configuration file with example settings.
    
    Args:
        file_path: Path where to create the configuration file
    """
    default_config = {
        "host": "0.0.0.0",
        "port": 8000,
        "database_type": "duckdb",
        "database_url": "./fhir4ds_server.db",
        "max_resources_per_request": 1000,
        "enable_parallel_processing": True,
        "predefined_views_file": "predefined_views.json"
    }
    
    with open(file_path, 'w') as f:
        json.dump(default_config, f, indent=2)
    
    print(f"Created default configuration file: {file_path}")


def create_example_views_file(file_path: str = "predefined_views.json"):
    """
    Create an example predefined views file.
    
    Args:
        file_path: Path where to create the views file
    """
    example_views = {
        "views": [
            {
                "name": "patient_demographics",
                "description": "Basic patient demographics",
                "resource": "Patient",
                "select": [{
                    "column": [
                        {"name": "patient_id", "path": "id", "type": "id"},
                        {"name": "family_name", "path": "name.family", "type": "string"},
                        {"name": "given_name", "path": "name.given", "type": "string"},
                        {"name": "birth_date", "path": "birthDate", "type": "date"},
                        {"name": "gender", "path": "gender", "type": "string"}
                    ]
                }]
            },
            {
                "name": "vital_signs",
                "description": "Patient vital signs observations",
                "resource": "Observation",
                "where": [{"path": "code.coding.code", "value": "29463-7"}],
                "select": [{
                    "column": [
                        {"name": "observation_id", "path": "id", "type": "id"},
                        {"name": "patient_reference", "path": "subject.reference", "type": "string"},
                        {"name": "measurement_value", "path": "valueQuantity.value", "type": "decimal"},
                        {"name": "measurement_unit", "path": "valueQuantity.unit", "type": "string"},
                        {"name": "measurement_date", "path": "effectiveDateTime", "type": "dateTime"}
                    ]
                }]
            }
        ]
    }
    
    with open(file_path, 'w') as f:
        json.dump(example_views, f, indent=2)
    
    print(f"Created example views file: {file_path}")