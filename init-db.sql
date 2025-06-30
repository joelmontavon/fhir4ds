-- FHIR4DS PostgreSQL Initialization Script
-- Sets up the database for FHIR resource storage

-- Enable JSON support (should be available by default in PostgreSQL 9.3+)
-- Create JSONB extensions if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create the main FHIR resources table
-- This will be created automatically by FHIR4DS, but we can pre-create it for better performance
CREATE TABLE IF NOT EXISTS fhir_resources (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    resource_id TEXT,
    resource_type TEXT,
    resource JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_fhir_resources_resource_type ON fhir_resources(resource_type);
CREATE INDEX IF NOT EXISTS idx_fhir_resources_resource_id ON fhir_resources(resource_id);
CREATE INDEX IF NOT EXISTS idx_fhir_resources_created_at ON fhir_resources(created_at);

-- Create GIN index on JSONB column for fast JSON queries
CREATE INDEX IF NOT EXISTS idx_fhir_resources_resource_gin ON fhir_resources USING GIN(resource);

-- Grant permissions to fhir4ds user
GRANT ALL PRIVILEGES ON TABLE fhir_resources TO fhir4ds;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO fhir4ds;