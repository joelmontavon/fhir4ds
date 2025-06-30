# FHIR4DS Docker Deployment Guide

This guide covers how to run the FHIR4DS Analytics Server using Docker for production and development environments.

## Quick Start

### Option 1: DuckDB (Lightweight, Default)
```bash
# Build and run with DuckDB (recommended for development/testing)
docker-compose --profile duckdb up --build

# Or using default profile
docker-compose up --build
```

### Option 2: PostgreSQL (Production)
```bash
# Build and run with PostgreSQL (recommended for production)
docker-compose --profile postgresql up --build
```

### Option 3: Development Mode
```bash
# Run in development mode with hot reload
docker-compose --profile dev up --build
```

## Deployment Options

### 1. DuckDB Deployment (Default)
**Best for**: Development, testing, small datasets, single-user scenarios

```bash
docker-compose --profile duckdb up -d
```

**Features**:
- ✅ Zero configuration required
- ✅ Persistent data storage in Docker volume
- ✅ Fast startup time
- ✅ Perfect for development and testing

**Access**: http://localhost:8000

### 2. PostgreSQL Deployment
**Best for**: Production, multi-user environments, large datasets

```bash
docker-compose --profile postgresql up -d
```

**Features**:
- ✅ Production-grade PostgreSQL database
- ✅ Optimized for concurrent access
- ✅ JSONB indexes for fast queries
- ✅ Automatic database initialization

**Access**: 
- FHIR4DS API: http://localhost:8000
- PostgreSQL: localhost:5432

### 3. Development Mode
**Best for**: Active development with code changes

```bash
docker-compose --profile dev up
```

**Features**:
- ✅ Hot reload on code changes
- ✅ Source code mounted as volume
- ✅ Development dependencies included

## Docker Image Details

### Multi-stage Build
The Dockerfile uses a multi-stage build for optimization:

1. **Builder stage**: Installs dependencies and builds the application
2. **Production stage**: Minimal runtime image with only necessary components

### Image Size Optimization
- Uses `python:3.11-slim` base image
- Multi-stage build reduces final image size
- Only production dependencies in final image
- Non-root user for security

### Dependencies Included
- **Core**: DuckDB, pandas, typing-extensions
- **Server**: FastAPI, uvicorn, pydantic  
- **PostgreSQL**: psycopg2-binary
- **Helpers**: openpyxl, pyarrow (optional)

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_TYPE` | `duckdb` | Database type (`duckdb` or `postgresql`) |
| `DATABASE_PATH` | `/app/data/fhir4ds.db` | DuckDB database file path |
| `DATABASE_URL` | - | PostgreSQL connection string |
| `VIEWS_DIR` | `/app/views` | Directory containing ViewDefinition files |
| `HOST` | `0.0.0.0` | Server host |
| `PORT` | `8000` | Server port |
| `RELOAD` | `false` | Enable auto-reload (dev mode) |

### Custom Configuration

1. **Custom ViewDefinitions**:
   ```bash
   # Create a views directory
   mkdir views
   # Add your .json ViewDefinition files
   cp my_views/*.json views/
   ```

2. **Custom Database**:
   ```bash
   # For PostgreSQL
   export DATABASE_URL="postgresql://user:pass@host:5432/db"
   docker-compose --profile postgresql up
   ```

3. **Custom Port**:
   ```bash
   # Edit docker-compose.yml ports section
   ports:
     - "9000:8000"  # Host:Container
   ```

## Data Persistence

### DuckDB
- Data stored in Docker volume `fhir4ds-data`
- Database file: `/app/data/fhir4ds.db`
- Survives container restarts

### PostgreSQL
- Data stored in Docker volume `postgres-data`
- Automatic database initialization
- User: `fhir4ds`, Password: `fhir4ds_password`, DB: `fhir4ds`

## Health Checks

Both services include health checks:
- **Endpoint**: `http://localhost:8000/health`
- **Interval**: 30 seconds
- **Timeout**: 10 seconds
- **Start period**: 40 seconds

## Production Deployment

### Recommended Production Setup

```yaml
# production-docker-compose.yml
version: '3.8'
services:
  fhir4ds:
    image: your-registry/fhir4ds:0.4.0
    ports:
      - "80:8000"
    environment:
      - DATABASE_TYPE=postgresql
      - DATABASE_URL=postgresql://user:password@your-db:5432/fhir4ds
    restart: always
    deploy:
      replicas: 2
      resources:
        limits:
          memory: 1G
          cpus: '0.5'
```

### Security Considerations

1. **Use external PostgreSQL** in production
2. **Change default passwords**
3. **Use secrets management** for credentials
4. **Enable TLS/SSL** for external access
5. **Configure firewall rules**
6. **Regular security updates**

### Scaling

```bash
# Scale horizontally
docker-compose --profile postgresql up --scale fhir4ds-postgresql=3

# Use load balancer (nginx, traefik, etc.)
# Configure database connection pooling
```

## Commands

### Build Commands
```bash
# Build image only
docker build -t fhir4ds:0.4.0 .

# Build with specific target
docker build --target production -t fhir4ds:prod .
```

### Runtime Commands
```bash
# Start services
docker-compose up -d

# View logs
docker-compose logs -f fhir4ds-duckdb

# Execute commands in container
docker-compose exec fhir4ds-duckdb python -c "import fhir4ds; print(fhir4ds.__version__)"

# Stop services
docker-compose down

# Remove volumes (deletes data!)
docker-compose down -v
```

### Maintenance Commands
```bash
# Update images
docker-compose pull
docker-compose up -d

# Backup DuckDB data
docker-compose exec fhir4ds-duckdb cp /app/data/fhir4ds.db /tmp/
docker cp $(docker-compose ps -q fhir4ds-duckdb):/tmp/fhir4ds.db ./backup.db

# Backup PostgreSQL data
docker-compose exec postgres pg_dump -U fhir4ds fhir4ds > backup.sql
```

## Troubleshooting

### Common Issues

1. **Port already in use**:
   ```bash
   # Change port in docker-compose.yml
   ports:
     - "8001:8000"
   ```

2. **Permission denied**:
   ```bash
   # Check file permissions
   chmod -R 755 views/
   ```

3. **Database connection failed**:
   ```bash
   # Check PostgreSQL is running
   docker-compose logs postgres
   
   # Test connection
   docker-compose exec postgres psql -U fhir4ds -d fhir4ds
   ```

4. **Out of memory**:
   ```bash
   # Increase memory limits
   deploy:
     resources:
       limits:
         memory: 2G
   ```

### Debugging

```bash
# Get container shell
docker-compose exec fhir4ds-duckdb bash

# Check application logs
docker-compose logs fhir4ds-duckdb

# Monitor resource usage
docker stats

# Inspect volumes
docker volume inspect fhir4ds_fhir4ds-data
```

## Performance Tuning

### For Large Datasets
1. Use PostgreSQL profile
2. Increase memory limits
3. Configure connection pooling
4. Use SSD storage for volumes
5. Optimize PostgreSQL configuration

### For High Concurrency
1. Scale horizontally with load balancer
2. Use external PostgreSQL with read replicas
3. Configure caching (Redis)
4. Implement rate limiting

## API Documentation

Once running, access the interactive API documentation:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/openapi.json

## Support

For issues with Docker deployment:
1. Check this documentation
2. Review logs: `docker-compose logs`
3. Verify configuration
4. Check GitHub issues
5. Create new issue with logs and configuration