"""
FHIR4DS Analytics Server

FastAPI application providing RESTful API for FHIR analytics as a service.
"""

import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from contextlib import asynccontextmanager

from .config import FHIRAnalyticsServerConfig, load_predefined_views, get_database_connection_string
from .models import (
    ViewDefinitionRequest, ViewDefinitionResponse, ViewListResponse,
    AnalyticsRequest, AnalyticsResponse, BulkResourceRequest, BulkResourceResponse,
    ServerInfo, HealthResponse, ErrorResponse, OutputFormat
)

from ..helpers import QuickConnect
from ..datastore import FHIRDataStore


class FHIRAnalyticsServer:
    """FHIR Analytics Server implementation"""
    
    def __init__(self, config: FHIRAnalyticsServerConfig):
        self.config = config
        self.db = None
        self.start_time = time.time()
        self.queries_executed = 0
        
        # Set up logging
        log_level = getattr(logging, config.log_level.upper(), logging.INFO)
        logging.basicConfig(
            level=log_level,
            filename=config.log_file,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    async def startup(self):
        """Initialize the server on startup"""
        self.logger.info("Starting FHIR4DS Analytics Server...")
        
        try:
            # Initialize database connection
            connection_string = get_database_connection_string(self.config)
            
            if self.config.database_type.lower() == "postgresql":
                self.db = QuickConnect.postgresql(connection_string, self.config.initialize_db)
            else:  # DuckDB
                self.db = QuickConnect.duckdb(connection_string, self.config.initialize_db)
            
            self.logger.info(f"Connected to {self.config.database_type} database: {connection_string}")
            
            # Create views management table
            await self._create_views_table()
            
            # Load predefined ViewDefinitions
            if self.config.predefined_views_file:
                await self._load_predefined_views()
            
            self.logger.info("FHIR4DS Analytics Server started successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to start server: {e}")
            raise
    
    async def shutdown(self):
        """Clean up on server shutdown"""
        self.logger.info("Shutting down FHIR4DS Analytics Server...")
        # Database connections will be automatically cleaned up
    
    async def _create_views_table(self):
        """Create table for storing ViewDefinitions"""
        try:
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.config.views_table_name} (
                    name VARCHAR PRIMARY KEY,
                    description VARCHAR,
                    resource VARCHAR NOT NULL,
                    status VARCHAR DEFAULT 'active',
                    view_definition JSON NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            
            if hasattr(self.db.datastore, 'execute_query'):
                self.db.datastore.execute_query(create_sql)
            elif hasattr(self.db.datastore.dialect, 'connection'):
                self.db.datastore.dialect.connection.execute(create_sql)
            
            self.logger.info(f"Created/verified views table: {self.config.views_table_name}")
            
        except Exception as e:
            self.logger.error(f"Failed to create views table: {e}")
            raise
    
    async def _load_predefined_views(self):
        """Load predefined ViewDefinitions from configuration file"""
        try:
            views = load_predefined_views(self.config.predefined_views_file)
            loaded_count = 0
            
            for view_data in views:
                try:
                    view_request = ViewDefinitionRequest(**view_data)
                    await self.create_view_definition(view_request)
                    loaded_count += 1
                    self.logger.info(f"Loaded predefined view: {view_request.name}")
                except Exception as e:
                    self.logger.warning(f"Failed to load predefined view {view_data.get('name', 'unknown')}: {e}")
            
            self.logger.info(f"Loaded {loaded_count} predefined ViewDefinitions")
            
        except Exception as e:
            self.logger.error(f"Failed to load predefined views: {e}")
    
    async def create_view_definition(self, view_request: ViewDefinitionRequest) -> ViewDefinitionResponse:
        """Create a new ViewDefinition"""
        try:
            # Check if view already exists
            existing = await self.get_view_definition(view_request.name)
            if existing:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"ViewDefinition '{view_request.name}' already exists"
                )
            
            # Convert to full ViewDefinition format
            view_definition = {
                "name": view_request.name,
                "resource": view_request.resource,
                "status": view_request.status or "active",
                "select": view_request.select
            }
            
            if view_request.where:
                view_definition["where"] = view_request.where
            
            # Insert into database
            now = datetime.now()
            insert_sql = f"""
                INSERT INTO {self.config.views_table_name} 
                (name, description, resource, status, view_definition, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            
            if hasattr(self.db.datastore, 'execute_query'):
                self.db.datastore.execute_query(insert_sql, (
                    view_request.name,
                    view_request.description,
                    view_request.resource,
                    view_request.status or "active",
                    json.dumps(view_definition),
                    now,
                    now
                ))
            elif hasattr(self.db.datastore.dialect, 'connection'):
                self.db.datastore.dialect.connection.execute(insert_sql, (
                    view_request.name,
                    view_request.description,
                    view_request.resource,
                    view_request.status or "active",
                    json.dumps(view_definition),
                    now,
                    now
                ))
            
            return ViewDefinitionResponse(
                name=view_request.name,
                description=view_request.description,
                resource=view_request.resource,
                status=view_request.status or "active",
                created_at=now,
                updated_at=now,
                select=view_request.select,
                where=view_request.where
            )
            
        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Failed to create ViewDefinition '{view_request.name}': {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create ViewDefinition: {str(e)}"
            )
    
    async def get_view_definition(self, name: str) -> Optional[ViewDefinitionResponse]:
        """Get a ViewDefinition by name"""
        try:
            select_sql = f"SELECT * FROM {self.config.views_table_name} WHERE name = ?"
            
            if hasattr(self.db.datastore, 'execute_query'):
                result = self.db.datastore.execute_query(select_sql, (name,))
            elif hasattr(self.db.datastore.dialect, 'connection'):
                result = self.db.datastore.dialect.connection.execute(select_sql, (name,)).fetchall()
            
            if not result:
                return None
            
            row = result[0]
            view_definition = json.loads(row[4])  # view_definition column
            
            return ViewDefinitionResponse(
                name=row[0],
                description=row[1],
                resource=row[2],
                status=row[3],
                created_at=row[5],
                updated_at=row[6],
                select=view_definition.get("select", []),
                where=view_definition.get("where")
            )
            
        except Exception as e:
            self.logger.error(f"Failed to get ViewDefinition '{name}': {e}")
            return None
    
    async def list_view_definitions(self) -> ViewListResponse:
        """List all ViewDefinitions"""
        try:
            select_sql = f"SELECT * FROM {self.config.views_table_name} ORDER BY name"
            
            if hasattr(self.db.datastore, 'execute_query'):
                results = self.db.datastore.execute_query(select_sql)
            elif hasattr(self.db.datastore.dialect, 'connection'):
                results = self.db.datastore.dialect.connection.execute(select_sql).fetchall()
            
            views = []
            for row in results or []:
                try:
                    view_definition = json.loads(row[4])
                    views.append(ViewDefinitionResponse(
                        name=row[0],
                        description=row[1],
                        resource=row[2],
                        status=row[3],
                        created_at=row[5],
                        updated_at=row[6],
                        select=view_definition.get("select", []),
                        where=view_definition.get("where")
                    ))
                except Exception as e:
                    self.logger.warning(f"Failed to parse ViewDefinition {row[0]}: {e}")
            
            return ViewListResponse(views=views, total_count=len(views))
            
        except Exception as e:
            self.logger.error(f"Failed to list ViewDefinitions: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to list ViewDefinitions: {str(e)}"
            )
    
    async def delete_view_definition(self, name: str) -> bool:
        """Delete a ViewDefinition"""
        try:
            # Check if view exists
            existing = await self.get_view_definition(name)
            if not existing:
                return False
            
            delete_sql = f"DELETE FROM {self.config.views_table_name} WHERE name = ?"
            
            if hasattr(self.db.datastore, 'execute_query'):
                self.db.datastore.execute_query(delete_sql, (name,))
            elif hasattr(self.db.datastore.dialect, 'connection'):
                self.db.datastore.dialect.connection.execute(delete_sql, (name,))
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to delete ViewDefinition '{name}': {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete ViewDefinition: {str(e)}"
            )
    
    async def execute_analytics(self, view_name: str, analytics_request: AnalyticsRequest) -> AnalyticsResponse:
        """Execute analytics using a ViewDefinition"""
        try:
            start_time = time.time()
            
            # Get ViewDefinition
            view_def = await self.get_view_definition(view_name)
            if not view_def:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"ViewDefinition '{view_name}' not found"
                )
            
            # Validate resource count
            if len(analytics_request.resources) > self.config.max_resources_per_request:
                raise HTTPException(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    detail=f"Too many resources. Maximum allowed: {self.config.max_resources_per_request}"
                )
            
            # Load resources into database temporarily or use in-memory processing
            if self.config.enable_parallel_processing:
                self.db.load_resources(
                    analytics_request.resources, 
                    parallel=True, 
                    batch_size=self.config.batch_size
                )
            else:
                for resource in analytics_request.resources:
                    self.db.load_resource(resource)
            
            # Create ViewDefinition dictionary for execution
            view_definition_dict = {
                "name": view_def.name,
                "resource": view_def.resource,
                "status": view_def.status,
                "select": view_def.select
            }
            
            if view_def.where:
                view_definition_dict["where"] = view_def.where
            
            # Execute ViewDefinition
            if analytics_request.format == OutputFormat.JSON:
                result = self.db.execute_to_dataframe(view_definition_dict, analytics_request.include_metadata)
                data = result.to_dict('records') if hasattr(result, 'to_dict') else []
            elif analytics_request.format == OutputFormat.CSV:
                # Execute and convert to CSV
                result = self.db.execute_to_dataframe(view_definition_dict, analytics_request.include_metadata)
                data = result.to_csv(index=False) if hasattr(result, 'to_csv') else ""
            else:
                # For other formats, use dataframe and convert
                result = self.db.execute_to_dataframe(view_definition_dict, analytics_request.include_metadata)
                data = result.to_dict('records') if hasattr(result, 'to_dict') else []
            
            execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
            self.queries_executed += 1
            
            # Prepare metadata
            metadata = None
            if analytics_request.include_metadata:
                metadata = {
                    "view_definition": view_definition_dict,
                    "database_type": self.config.database_type,
                    "parallel_processing": self.config.enable_parallel_processing,
                    "batch_size": self.config.batch_size
                }
            
            return AnalyticsResponse(
                view_name=view_name,
                executed_at=datetime.now(),
                resource_count=len(analytics_request.resources),
                result_count=len(data) if isinstance(data, list) else 1,
                execution_time_ms=execution_time,
                format=analytics_request.format,
                data=data,
                metadata=metadata
            )
            
        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Failed to execute analytics for view '{view_name}': {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to execute analytics: {str(e)}"
            )
    
    async def bulk_load_resources(self, bulk_request: BulkResourceRequest) -> BulkResourceResponse:
        """Bulk load resources into the database"""
        try:
            start_time = time.time()
            
            # Validate resource count
            if len(bulk_request.resources) > self.config.max_resources_per_request:
                raise HTTPException(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    detail=f"Too many resources. Maximum allowed: {self.config.max_resources_per_request}"
                )
            
            # Load resources
            if bulk_request.parallel and self.config.enable_parallel_processing:
                self.db.load_resources(
                    bulk_request.resources,
                    parallel=True,
                    batch_size=bulk_request.batch_size
                )
            else:
                for resource in bulk_request.resources:
                    self.db.load_resource(resource)
            
            processing_time = (time.time() - start_time) * 1000
            
            return BulkResourceResponse(
                loaded_count=len(bulk_request.resources),
                processing_time_ms=processing_time,
                parallel_processing=bulk_request.parallel and self.config.enable_parallel_processing,
                batch_size=bulk_request.batch_size
            )
            
        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Failed to bulk load resources: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to bulk load resources: {str(e)}"
            )
    
    async def get_server_info(self) -> ServerInfo:
        """Get server information and statistics"""
        try:
            uptime = time.time() - self.start_time
            
            # Get view count
            views_response = await self.list_view_definitions()
            total_views = views_response.total_count
            
            # Get resource count
            total_resources = self.db.get_resource_count() if self.db else 0
            
            features = [
                "SQL-on-FHIR v2.0",
                "Dual Database Support (DuckDB, PostgreSQL)",
                "Parallel Processing",
                "Multi-Format Export",
                "Database Object Creation",
                "Real-time Analytics API"
            ]
            
            return ServerInfo(
                database_type=self.config.database_type,
                database_url=self.config.database_url,
                uptime_seconds=uptime,
                total_views=total_views,
                total_resources=total_resources,
                total_queries_executed=self.queries_executed,
                features=features
            )
            
        except Exception as e:
            self.logger.error(f"Failed to get server info: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get server info: {str(e)}"
            )


def create_app(config: FHIRAnalyticsServerConfig = None) -> FastAPI:
    """Create and configure the FastAPI application"""
    
    if config is None:
        config = FHIRAnalyticsServerConfig()
    
    # Create server instance
    server = FHIRAnalyticsServer(config)
    
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Startup
        await server.startup()
        yield
        # Shutdown
        await server.shutdown()
    
    # Create FastAPI app
    app = FastAPI(
        title="FHIR4DS Analytics API",
        description="Production-ready FHIR analytics as a service",
        version="3.0.0",
        lifespan=lifespan
    )
    
    # Add CORS middleware
    if config.enable_cors:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    
    # Health check endpoint
    @app.get("/health", response_model=HealthResponse)
    async def health_check():
        """Health check endpoint"""
        try:
            db_connected = server.db is not None and server.db.test_connection()
            return HealthResponse(
                timestamp=datetime.now(),
                database_connected=db_connected
            )
        except Exception:
            return HealthResponse(
                status="unhealthy",
                timestamp=datetime.now(),
                database_connected=False
            )
    
    # Server info endpoint
    @app.get("/info", response_model=ServerInfo)
    async def get_info():
        """Get server information and statistics"""
        return await server.get_server_info()
    
    # ViewDefinition management endpoints
    @app.post("/views", response_model=ViewDefinitionResponse, status_code=status.HTTP_201_CREATED)
    async def create_view(view_request: ViewDefinitionRequest):
        """Create a new ViewDefinition"""
        return await server.create_view_definition(view_request)
    
    @app.get("/views", response_model=ViewListResponse)
    async def list_views():
        """List all ViewDefinitions"""
        return await server.list_view_definitions()
    
    @app.get("/views/{view_name}", response_model=ViewDefinitionResponse)
    async def get_view(view_name: str):
        """Get a specific ViewDefinition"""
        view_def = await server.get_view_definition(view_name)
        if not view_def:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"ViewDefinition '{view_name}' not found"
            )
        return view_def
    
    @app.delete("/views/{view_name}", status_code=status.HTTP_204_NO_CONTENT)
    async def delete_view(view_name: str):
        """Delete a ViewDefinition"""
        deleted = await server.delete_view_definition(view_name)
        if not deleted:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"ViewDefinition '{view_name}' not found"
            )
    
    # Analytics execution endpoint
    @app.post("/views/{view_name}/execute", response_model=AnalyticsResponse)
    async def execute_analytics(
        view_name: str, 
        analytics_request: AnalyticsRequest,
        format: Optional[OutputFormat] = Query(OutputFormat.JSON, description="Output format")
    ):
        """Execute analytics using a ViewDefinition"""
        # Override format from query parameter if provided
        if format:
            analytics_request.format = format
        
        return await server.execute_analytics(view_name, analytics_request)
    
    # Bulk resource loading endpoint
    @app.post("/resources", response_model=BulkResourceResponse)
    async def bulk_load_resources(bulk_request: BulkResourceRequest):
        """Bulk load FHIR resources into the database"""
        return await server.bulk_load_resources(bulk_request)
    
    return app