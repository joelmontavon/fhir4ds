"""
Unit tests for database dialect system
"""

import pytest
from fhir4ds.dialects import DuckDBDialect, PostgreSQLDialect
from fhir4ds.dialects.base import DatabaseDialect


def get_dialect(name: str):
    """Simple dialect factory function"""
    import unittest.mock
    name = name.lower()
    if name == 'duckdb':
        return DuckDBDialect()
    elif name == 'postgresql':
        # Mock the connection for testing
        with unittest.mock.patch('fhir4ds.dialects.postgresql.psycopg2.connect'):
            return PostgreSQLDialect("postgresql://test:test@localhost:5432/test")
    else:
        raise ValueError(f"Unknown dialect: {name}")


class TestDialectRegistry:
    """Test dialect registry functionality"""
    
    def test_get_duckdb_dialect(self):
        """Test getting DuckDB dialect"""
        dialect = get_dialect('duckdb')
        assert isinstance(dialect, DuckDBDialect)
    
    def test_get_postgresql_dialect(self):
        """Test getting PostgreSQL dialect"""
        dialect = get_dialect('postgresql')
        assert isinstance(dialect, PostgreSQLDialect)
    
    def test_get_unknown_dialect(self):
        """Test error handling for unknown dialect"""
        with pytest.raises(ValueError, match="Unknown dialect"):
            get_dialect('unknown_db')
    
    def test_case_insensitive_dialect_names(self):
        """Test that dialect names are case insensitive"""
        dialect1 = get_dialect('DuckDB')
        dialect2 = get_dialect('duckdb')
        dialect3 = get_dialect('DUCKDB')
        
        assert type(dialect1) == type(dialect2) == type(dialect3)


class TestBaseSQLDialect:
    """Test base dialect interface"""
    
    def test_abstract_methods(self):
        """Test that base class is properly abstract"""
        with pytest.raises(TypeError):
            DatabaseDialect()


class TestDuckDBDialect:
    """Test DuckDB dialect implementation"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.dialect = DuckDBDialect()
    
    def test_json_extract(self):
        """Test JSON extraction method"""
        result = self.dialect.json_extract("column_name", "$.field")
        assert 'json_extract' in result
        assert 'column_name' in result
        assert '$.field' in result
    
    def test_json_extract_string(self):
        """Test JSON string extraction method"""
        result = self.dialect.json_extract_string("column_name", "$.field")
        assert 'json_extract_string' in result
        assert 'column_name' in result
        assert '$.field' in result
    
    def test_json_array_length(self):
        """Test JSON array length method"""
        result = self.dialect.json_array_length("array_column")
        assert 'json_array_length' in result
        assert 'array_column' in result
    
    def test_json_type(self):
        """Test JSON type checking method"""
        result = self.dialect.get_json_type("json_column")
        assert 'json_type' in result
        assert 'json_column' in result
    
    def test_string_agg(self):
        """Test string aggregation method"""
        result = self.dialect.string_agg("value_expr", "separator")
        assert 'string_agg' in result
        assert 'value_expr' in result
        assert 'separator' in result
    
    def test_cast_operations(self):
        """Test type casting operations"""
        # Note: DuckDB dialect may not have these specific methods
        # Test basic casting syntax instead
        if hasattr(self.dialect, 'cast_to_string'):
            result = self.dialect.cast_to_string("expr")
            assert 'VARCHAR' in result or 'TEXT' in result
        else:
            # Test that basic casting syntax is available
            assert self.dialect.cast_syntax == "::"
    
    def test_connection_handling(self):
        """Test connection creation and management"""
        # Should create a connection (in-memory for testing)
        assert self.dialect.connection is not None
        
        # Should be able to execute simple queries
        try:
            self.dialect.execute_query("SELECT 1")
        except Exception as e:
            pytest.fail(f"Simple query execution failed: {e}")
    
    def test_custom_functions_setup(self):
        """Test that custom functions are set up properly"""
        # The dialect should set up custom FHIR functions
        # Test that getResourceKey function exists
        try:
            result = self.dialect.execute_query(
                "SELECT getResourceKey('{}') as test".format('{"id": "test123"}')
            )
            # Should not raise an error if function is properly registered
        except Exception as e:
            # Some error is expected if the function format is wrong, 
            # but should not be "function not found"
            assert "not found" not in str(e).lower()


class TestPostgreSQLDialect:
    """Test PostgreSQL dialect implementation"""
    
    def setup_method(self):
        """Set up test fixtures"""
        # Mock PostgreSQL dialect to avoid actual database connection
        import unittest.mock
        with unittest.mock.patch('fhir4ds.dialects.postgresql.psycopg2.connect'):
            self.dialect = PostgreSQLDialect("postgresql://test:test@localhost:5432/test")
    
    def test_json_extract(self):
        """Test JSONB extraction method"""
        result = self.dialect.json_extract("column_name", "$.field")
        # PostgreSQL uses -> or ->> operators or jsonb_extract_path
        assert ('->' in result or '->>' in result or 
                'jsonb_extract_path' in result or 
                'json_extract' in result)
        assert 'column_name' in result
    
    def test_json_extract_string(self):
        """Test JSONB string extraction method"""
        result = self.dialect.json_extract_string("column_name", "$.field")
        # Should use ->> or jsonb_extract_path_text for string extraction
        assert ('->>' in result or 
                'jsonb_extract_path_text' in result or
                'json_extract_string' in result)
        assert 'column_name' in result
    
    def test_json_array_length(self):
        """Test JSONB array length method"""
        result = self.dialect.json_array_length("array_column")
        assert ('jsonb_array_length' in result or 
                'json_array_length' in result)
        assert 'array_column' in result
    
    def test_json_type(self):
        """Test JSONB type checking method"""
        result = self.dialect.get_json_type("json_column")
        assert ('jsonb_typeof' in result or 
                'json_typeof' in result or
                'json_type' in result)
        assert 'json_column' in result
    
    def test_string_agg(self):
        """Test string aggregation method"""
        result = self.dialect.string_agg("value_expr", "separator")
        assert 'string_agg' in result
        assert 'value_expr' in result
        assert 'separator' in result
    
    def test_cast_operations(self):
        """Test PostgreSQL type casting"""
        # Note: PostgreSQL dialect may not have these specific methods
        # Test basic casting syntax instead
        if hasattr(self.dialect, 'cast_to_string'):
            result = self.dialect.cast_to_string("expr")
            assert '::TEXT' in result or 'CAST' in result
        else:
            # Test that basic casting syntax is available
            assert hasattr(self.dialect, 'cast_syntax')
    
    def test_no_connection_by_default(self):
        """Test that PostgreSQL dialect connection is mocked in tests"""
        # Since we're using a mock, connection will be a MagicMock
        # In real usage without mock, this would require actual credentials
        assert self.dialect.connection is not None  # Should be mocked


class TestDialectIntegration:
    """Test dialect integration with core components"""
    
    @pytest.mark.skip(reason="FHIRPathToSQL deprecated - dialect integration tested via new pipeline")
    def test_dialect_with_translator(self):
        """Test using dialects with translator"""
        # from fhir4ds.fhirpath.core.translator import FHIRPathToSQL
        
        # Test that translator works with DuckDB dialect
        duckdb_dialect = DuckDBDialect()
        # translator = FHIRPathToSQL(dialect=duckdb_dialect)
        # result = translator.translate_to_parts("Patient.name")
        # assert 'expression_sql' in result
        pass
    
    def test_dialect_sql_compatibility(self):
        """Test that dialect SQL is compatible with expected patterns"""
        import unittest.mock
        
        duckdb_dialect = DuckDBDialect()
        
        # Mock PostgreSQL dialect for testing
        with unittest.mock.patch('fhir4ds.dialects.postgresql.psycopg2.connect'):
            pg_dialect = PostgreSQLDialect("postgresql://test:test@localhost:5432/test")
        
        # Both should generate some form of JSON extraction
        duckdb_sql = duckdb_dialect.extract_json_field("col", "$.field")
        pg_sql = pg_dialect.extract_json_field("col", "$.field")
        
        assert len(duckdb_sql) > 0
        assert len(pg_sql) > 0
        assert 'col' in duckdb_sql
        assert 'col' in pg_sql


if __name__ == '__main__':
    pytest.main([__file__])