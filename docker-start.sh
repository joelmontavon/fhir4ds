#!/bin/bash
# FHIR4DS Docker Quick Start Script
# This script helps you get started with FHIR4DS using Docker

set -e

echo "🚀 FHIR4DS Docker Quick Start"
echo "============================="

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

print_step() {
    echo -e "${BLUE}📋 Step $1: $2${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check if Docker is installed
print_step 1 "Checking Docker installation"
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed. Please install Docker first:"
    echo "  - macOS/Windows: https://docs.docker.com/desktop/"
    echo "  - Linux: https://docs.docker.com/engine/install/"
    exit 1
fi

if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    print_error "Docker Compose is not available. Please install Docker Compose:"
    echo "  - https://docs.docker.com/compose/install/"
    exit 1
fi

print_success "Docker and Docker Compose are available"

# Check if docker daemon is running
print_step 2 "Checking Docker daemon"
if ! docker info &> /dev/null; then
    print_error "Docker daemon is not running. Please start Docker Desktop or the Docker daemon."
    exit 1
fi
print_success "Docker daemon is running"

# Choose deployment type
print_step 3 "Selecting deployment type"
echo ""
echo "Choose your deployment option:"
echo "1) DuckDB (Lightweight, recommended for development)"
echo "2) PostgreSQL (Production-grade, recommended for production)"
echo "3) Development mode (with hot reload)"
echo ""
read -p "Enter your choice (1-3): " choice

case $choice in
    1)
        PROFILE="duckdb"
        DESCRIPTION="DuckDB (lightweight)"
        ;;
    2)
        PROFILE="postgresql"
        DESCRIPTION="PostgreSQL (production)"
        ;;
    3)
        PROFILE="dev"
        DESCRIPTION="Development mode"
        ;;
    *)
        print_error "Invalid choice. Defaulting to DuckDB."
        PROFILE="duckdb"
        DESCRIPTION="DuckDB (lightweight)"
        ;;
esac

print_success "Selected: $DESCRIPTION"

# Build and start services
print_step 4 "Building and starting services"
echo "This may take a few minutes on first run..."

if command -v docker-compose &> /dev/null; then
    COMPOSE_CMD="docker-compose"
else
    COMPOSE_CMD="docker compose"
fi

echo "Running: $COMPOSE_CMD --profile $PROFILE up --build -d"

if $COMPOSE_CMD --profile $PROFILE up --build -d; then
    print_success "Services started successfully!"
else
    print_error "Failed to start services. Check the logs above for details."
    exit 1
fi

# Wait for services to be healthy
print_step 5 "Waiting for services to be ready"
echo "Checking service health..."

# Wait up to 60 seconds for the service to be ready
for i in {1..60}; do
    if curl -s -f http://localhost:8000/health > /dev/null 2>&1; then
        print_success "FHIR4DS server is ready!"
        break
    fi
    
    if [ $i -eq 60 ]; then
        print_warning "Service health check timed out. It may still be starting up."
        break
    fi
    
    echo -n "."
    sleep 1
done

echo ""

# Show status and next steps
print_step 6 "Deployment complete"
echo ""
print_success "🎉 FHIR4DS is now running!"
echo ""
echo "📋 Service Information:"
echo "  • API Server: http://localhost:8000"
echo "  • API Documentation: http://localhost:8000/docs"
echo "  • Health Check: http://localhost:8000/health"

if [ "$PROFILE" = "postgresql" ]; then
    echo "  • PostgreSQL: localhost:5432 (user: fhir4ds, db: fhir4ds)"
fi

echo ""
echo "📖 Common Commands:"
echo "  • View logs: $COMPOSE_CMD --profile $PROFILE logs -f"
echo "  • Stop services: $COMPOSE_CMD --profile $PROFILE down"
echo "  • Restart: $COMPOSE_CMD --profile $PROFILE restart"
echo ""

# Check if example views directory exists
if [ -d "views" ]; then
    print_success "Found views directory with ViewDefinitions"
else
    print_warning "No views directory found. Create one and add .json ViewDefinition files."
    echo "  mkdir views"
    echo "  # Add your ViewDefinition JSON files to views/"
fi

print_success "Setup complete! Visit http://localhost:8000/docs to get started."