#!/usr/bin/env python3
"""
FHIR4DS Analytics Server

Command-line interface for starting the FHIR4DS analytics server.
"""

import argparse
import sys
import uvicorn
from pathlib import Path

from .config import FHIRAnalyticsServerConfig, create_default_config_file, create_example_views_file
from .app import create_app


def create_argument_parser():
    """Create command line argument parser"""
    parser = argparse.ArgumentParser(
        description="FHIR4DS Analytics Server - Production-ready FHIR analytics as a service",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start server with default settings (DuckDB)
  python -m fhir4ds.server

  # Start with PostgreSQL
  python -m fhir4ds.server --database-type postgresql --database-url "postgresql://user:pass@localhost:5432/fhir"

  # Start with predefined views
  python -m fhir4ds.server --predefined-views ./my_views.json

  # Start in development mode
  python -m fhir4ds.server --reload --log-level DEBUG

  # Create example configuration files
  python -m fhir4ds.server --create-config
        """
    )
    
    # Server settings
    parser.add_argument(
        "--host", 
        default="0.0.0.0",
        help="Host to bind the server (default: 0.0.0.0)"
    )
    
    parser.add_argument(
        "--port", 
        type=int, 
        default=8000,
        help="Port to bind the server (default: 8000)"
    )
    
    parser.add_argument(
        "--workers", 
        type=int, 
        default=1,
        help="Number of worker processes (default: 1)"
    )
    
    parser.add_argument(
        "--reload", 
        action="store_true",
        help="Enable auto-reload for development"
    )
    
    # Database settings
    parser.add_argument(
        "--database-type", 
        choices=["duckdb", "postgresql"],
        default="duckdb",
        help="Database type (default: duckdb)"
    )
    
    parser.add_argument(
        "--database-url", 
        default="./fhir4ds_server.db",
        help="Database connection URL (default: ./fhir4ds_server.db)"
    )
    
    parser.add_argument(
        "--no-init-db", 
        action="store_true",
        help="Don't initialize database tables on startup"
    )
    
    # ViewDefinition settings
    parser.add_argument(
        "--predefined-views", 
        help="Path to JSON file with predefined ViewDefinitions"
    )
    
    parser.add_argument(
        "--views-table", 
        default="fhir4ds_views",
        help="Table name for storing ViewDefinitions (default: fhir4ds_views)"
    )
    
    # Performance settings
    parser.add_argument(
        "--max-resources", 
        type=int, 
        default=1000,
        help="Maximum resources per request (default: 1000)"
    )
    
    parser.add_argument(
        "--no-parallel", 
        action="store_true",
        help="Disable parallel processing"
    )
    
    parser.add_argument(
        "--batch-size", 
        type=int, 
        default=100,
        help="Batch size for parallel processing (default: 100)"
    )
    
    # Security and behavior
    parser.add_argument(
        "--api-key", 
        help="API key for authentication (optional)"
    )
    
    parser.add_argument(
        "--no-cors", 
        action="store_true",
        help="Disable CORS middleware"
    )
    
    # Logging
    parser.add_argument(
        "--log-level", 
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    
    parser.add_argument(
        "--log-file", 
        help="Log file path (default: console only)"
    )
    
    # Utility commands
    parser.add_argument(
        "--create-config", 
        action="store_true",
        help="Create example configuration files and exit"
    )
    
    parser.add_argument(
        "--config-file", 
        help="Load configuration from JSON file"
    )
    
    return parser


def create_config_from_args(args) -> FHIRAnalyticsServerConfig:
    """Create FHIRAnalyticsServerConfig from command line arguments"""
    
    config_dict = {
        "host": args.host,
        "port": args.port,
        "workers": args.workers,
        "reload": args.reload,
        "database_type": args.database_type,
        "database_url": args.database_url,
        "initialize_db": not args.no_init_db,
        "predefined_views_file": args.predefined_views,
        "views_table_name": args.views_table,
        "max_resources_per_request": args.max_resources,
        "enable_parallel_processing": not args.no_parallel,
        "batch_size": args.batch_size,
        "api_key": args.api_key,
        "enable_cors": not args.no_cors,
        "log_level": args.log_level,
        "log_file": args.log_file
    }
    
    # Filter out None values
    config_dict = {k: v for k, v in config_dict.items() if v is not None}
    
    return FHIRAnalyticsServerConfig(**config_dict)


def main():
    """Main entry point for the server"""
    
    parser = create_argument_parser()
    args = parser.parse_args()
    
    # Handle utility commands
    if args.create_config:
        print("Creating example configuration files...")
        create_default_config_file("fhir4ds_server_config.json")
        create_example_views_file("predefined_views.json")
        print("Files created:")
        print("  - fhir4ds_server_config.json (server configuration)")
        print("  - predefined_views.json (example ViewDefinitions)")
        print("\nEdit these files and restart the server with:")
        print("  python -m fhir4ds.server --predefined-views predefined_views.json")
        return
    
    # Create configuration
    try:
        config = create_config_from_args(args)
        
        # Validate configuration
        if config.database_type == "postgresql" and not config.database_url.startswith(('postgresql://', 'postgres://')):
            print("Error: PostgreSQL database type requires a valid PostgreSQL connection string")
            print("Example: postgresql://user:password@localhost:5432/database")
            sys.exit(1)
        
        # Check predefined views file
        if config.predefined_views_file and not Path(config.predefined_views_file).exists():
            print(f"Warning: Predefined views file not found: {config.predefined_views_file}")
            print("Server will start without predefined views.")
        
    except Exception as e:
        print(f"Configuration error: {e}")
        sys.exit(1)
    
    # Create FastAPI app
    try:
        app = create_app(config)
        print(f"FHIR4DS Analytics Server starting...")
        print(f"Database: {config.database_type} ({config.database_url})")
        print(f"Server: http://{config.host}:{config.port}")
        print(f"API Docs: http://{config.host}:{config.port}/docs")
        
        # Start server
        uvicorn.run(
            app,
            host=config.host,
            port=config.port,
            workers=config.workers if not config.reload else 1,
            reload=config.reload,
            log_level=config.log_level.lower(),
            access_log=True
        )
        
    except KeyboardInterrupt:
        print("\nServer stopped by user")
    except Exception as e:
        print(f"Server error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()