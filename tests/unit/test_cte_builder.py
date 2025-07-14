"""
Unit tests for CTEBuilder class.

Tests all functionality of the centralized CTE management system including:
- Basic CTE creation and referencing
- Dependency resolution and ordering
- Deduplication of identical CTEs
- Error handling for invalid states
- Debugging and validation features
"""

import pytest
from fhir4ds.fhirpath.core.cte_builder import CTEBuilder


class TestCTEBuilderBasicFunctionality:
    """Test basic CTE creation and referencing."""
    
    def test_create_cte_basic(self):
        """Test basic CTE creation with auto-generated names."""
        builder = CTEBuilder()
        
        # Create first CTE
        cte_name = builder.create_cte("test", "SELECT 1 as value")
        
        assert cte_name == "test_1"
        assert cte_name in builder.ctes
        assert builder.ctes[cte_name] == "SELECT 1 as value"
        assert builder.get_cte_count() == 1
        assert builder.has_ctes() is True
    
    def test_create_multiple_ctes(self):
        """Test creating multiple CTEs with incrementing names."""
        builder = CTEBuilder()
        
        cte1 = builder.create_cte("filter", "SELECT * FROM table1 WHERE active = true")
        cte2 = builder.create_cte("transform", "SELECT id, UPPER(name) as name FROM table2")
        cte3 = builder.create_cte("filter", "SELECT * FROM table3 WHERE deleted = false")
        
        assert cte1 == "filter_1"
        assert cte2 == "transform_2" 
        assert cte3 == "filter_3"
        assert builder.get_cte_count() == 3
        
        # Check all CTEs are stored
        assert "filter_1" in builder.ctes
        assert "transform_2" in builder.ctes
        assert "filter_3" in builder.ctes
    
    def test_cte_reference_full(self):
        """Test generating references to CTEs without column specification."""
        builder = CTEBuilder()
        cte_name = builder.create_cte("test", "SELECT id, name FROM patients")
        
        ref = builder.reference(cte_name)
        assert ref == "(SELECT * FROM test_1)"
    
    def test_cte_reference_specific_column(self):
        """Test generating references to specific CTE columns."""
        builder = CTEBuilder()
        cte_name = builder.create_cte("count", "SELECT COUNT(*) as patient_count FROM patients")
        
        ref = builder.reference(cte_name, "patient_count")
        assert ref == "(SELECT patient_count FROM count_1)"
    
    def test_reference_nonexistent_cte(self):
        """Test error handling when referencing non-existent CTE."""
        builder = CTEBuilder()
        
        with pytest.raises(ValueError, match="CTE 'nonexistent' not found"):
            builder.reference("nonexistent")
    
    def test_empty_sql_validation(self):
        """Test validation of empty SQL in CTE creation."""
        builder = CTEBuilder()
        
        with pytest.raises(ValueError, match="CTE SQL cannot be empty"):
            builder.create_cte("test", "")
        
        with pytest.raises(ValueError, match="CTE SQL cannot be empty"):
            builder.create_cte("test", "   ")
    
    def test_sql_whitespace_normalization(self):
        """Test that SQL whitespace is normalized."""
        builder = CTEBuilder()
        
        sql_with_extra_whitespace = """
        SELECT   id,   name
        FROM     patients
        WHERE    active = true
        """
        
        cte_name = builder.create_cte("test", sql_with_extra_whitespace)
        stored_sql = builder.ctes[cte_name]
        
        # Should be stripped but maintain internal structure
        assert not stored_sql.startswith('\n')
        assert not stored_sql.endswith('\n')
        assert "SELECT" in stored_sql
        assert "patients" in stored_sql


class TestCTEBuilderDependencyManagement:
    """Test dependency tracking and resolution."""
    
    def test_create_cte_with_dependencies(self):
        """Test creating CTEs with explicit dependencies."""
        builder = CTEBuilder()
        
        cte1 = builder.create_cte("base", "SELECT id FROM patients")
        cte2 = builder.create_cte("filtered", "SELECT * FROM base_1 WHERE active = true", [cte1])
        
        assert builder.dependencies[cte1] == []
        assert builder.dependencies[cte2] == [cte1]
    
    def test_dependency_resolution_simple(self):
        """Test simple dependency resolution ordering."""
        builder = CTEBuilder()
        
        cte1 = builder.create_cte("base", "SELECT id FROM patients")
        cte2 = builder.create_cte("filtered", "SELECT * FROM base_1 WHERE active = true", [cte1])
        
        ordered = builder._resolve_dependencies()
        
        # cte1 should come before cte2
        assert ordered.index(cte1) < ordered.index(cte2)
        assert len(ordered) == 2
    
    def test_dependency_resolution_complex(self):
        """Test complex dependency resolution with multiple levels."""
        builder = CTEBuilder()
        
        # Create CTEs in proper order to test dependency resolution
        cte_a = builder.create_cte("base", "SELECT id FROM patients")
        cte_b = builder.create_cte("filtered", f"SELECT * FROM {cte_a} WHERE active = true", [cte_a])
        cte_c = builder.create_cte("final", f"SELECT COUNT(*) FROM {cte_b}", [cte_b])
        
        ordered = builder._resolve_dependencies()
        
        # Should be ordered by dependencies: base -> filtered -> final
        base_idx = ordered.index(cte_a)
        filtered_idx = ordered.index(cte_b) 
        final_idx = ordered.index(cte_c)
        
        assert base_idx < filtered_idx < final_idx
    
    def test_circular_dependency_detection(self):
        """Test detection of circular dependencies."""
        builder = CTEBuilder()
        
        cte1 = builder.create_cte("first", "SELECT * FROM second_1")
        cte2 = builder.create_cte("second", "SELECT * FROM first_1")
        
        # Manually set up circular dependency
        builder.dependencies[cte1] = [cte2]
        builder.dependencies[cte2] = [cte1]
        
        with pytest.raises(ValueError, match="Circular dependency detected"):
            builder._resolve_dependencies()
    
    def test_missing_dependency_detection(self):
        """Test detection of missing dependencies."""
        builder = CTEBuilder()
        
        # Create CTE with dependency on non-existent CTE
        with pytest.raises(ValueError, match="Dependencies not found: \\['nonexistent'\\]"):
            builder.create_cte("test", "SELECT * FROM nonexistent", ["nonexistent"])
    
    def test_self_dependency_invalid(self):
        """Test that self-dependencies are handled properly."""
        builder = CTEBuilder()
        
        cte1 = builder.create_cte("base", "SELECT id FROM patients")
        
        # Try to create self-dependency
        builder.dependencies[cte1] = [cte1]
        
        with pytest.raises(ValueError, match="Circular dependency detected"):
            builder._resolve_dependencies()


class TestCTEBuilderDeduplication:
    """Test CTE deduplication functionality."""
    
    def test_add_cte_deduplication(self):
        """Test that identical CTEs are deduplicated."""
        builder = CTEBuilder()
        
        sql = "SELECT * FROM patients WHERE active = true"
        
        cte1 = builder.add_cte("filter1", sql)
        cte2 = builder.add_cte("filter2", sql)  # Same SQL, should be deduplicated
        
        assert cte1 == cte2  # Should return same name
        assert builder.get_cte_count() == 1  # Only one CTE should exist
        assert cte1 in builder.ctes
    
    def test_whitespace_normalization_in_deduplication(self):
        """Test that whitespace differences don't prevent deduplication."""
        builder = CTEBuilder()
        
        sql1 = "SELECT * FROM patients WHERE active = true"
        sql2 = "SELECT   *   FROM   patients   WHERE   active   =   true"
        
        cte1 = builder.add_cte("filter1", sql1)
        cte2 = builder.add_cte("filter2", sql2)
        
        assert cte1 == cte2  # Should be deduplicated despite whitespace differences
        assert builder.get_cte_count() == 1
    
    def test_dependency_merging_in_deduplication(self):
        """Test that dependencies are merged when CTEs are deduplicated."""
        builder = CTEBuilder()
        
        # Create base CTEs
        base1 = builder.create_cte("base1", "SELECT id FROM table1")
        base2 = builder.create_cte("base2", "SELECT id FROM table2")
        
        sql = "SELECT COUNT(*) FROM combined"
        
        # Add same CTE with different dependencies
        cte1 = builder.add_cte("counter1", sql, [base1])
        cte2 = builder.add_cte("counter2", sql, [base2])
        
        assert cte1 == cte2  # Should be deduplicated
        
        # Dependencies should be merged
        merged_deps = set(builder.dependencies[cte1])
        assert base1 in merged_deps
        assert base2 in merged_deps
    
    def test_create_cte_no_deduplication(self):
        """Test that create_cte doesn't perform deduplication."""
        builder = CTEBuilder()
        
        sql = "SELECT * FROM patients"
        
        cte1 = builder.create_cte("test", sql)
        cte2 = builder.create_cte("test", sql)  # Same SQL but should create new CTE
        
        assert cte1 != cte2  # Should be different names
        assert cte1 == "test_1"
        assert cte2 == "test_2"
        assert builder.get_cte_count() == 2


class TestCTEBuilderQueryBuilding:
    """Test final SQL query building."""
    
    def test_build_final_query_no_ctes(self):
        """Test building query when no CTEs exist."""
        builder = CTEBuilder()
        main_sql = "SELECT COUNT(*) FROM patients"
        
        result = builder.build_final_query(main_sql)
        assert result == main_sql  # Should return unchanged
    
    def test_build_final_query_single_cte(self):
        """Test building query with single CTE."""
        builder = CTEBuilder()
        
        cte_name = builder.create_cte("filter", "SELECT * FROM patients WHERE active = true")
        main_sql = f"SELECT COUNT(*) FROM {cte_name}"
        
        result = builder.build_final_query(main_sql)
        
        assert result.startswith("WITH")
        assert "filter_1 AS (SELECT * FROM patients WHERE active = true)" in result
        assert result.endswith(main_sql)
    
    def test_build_final_query_multiple_ctes_ordered(self):
        """Test building query with multiple CTEs in proper dependency order."""
        builder = CTEBuilder()
        
        cte1 = builder.create_cte("base", "SELECT id FROM patients")
        cte2 = builder.create_cte("filtered", f"SELECT * FROM {cte1} WHERE active = true", [cte1])
        cte3 = builder.create_cte("counted", f"SELECT COUNT(*) as total FROM {cte2}", [cte2])
        
        main_sql = f"SELECT total FROM {cte3}"
        result = builder.build_final_query(main_sql)
        
        # Check that WITH clause is present
        assert result.startswith("WITH")
        
        # Check that CTEs appear in dependency order
        base_pos = result.find("base_1 AS")
        filtered_pos = result.find("filtered_2 AS")
        counted_pos = result.find("counted_3 AS")
        
        assert base_pos < filtered_pos < counted_pos
        
        # Check main SQL is at the end
        assert result.endswith(main_sql)
    
    def test_build_final_query_complex_dependencies(self):
        """Test building query with complex dependency structure."""
        builder = CTEBuilder()
        
        # Create diamond dependency pattern: A -> B, A -> C, B -> D, C -> D
        cte_a = builder.create_cte("source", "SELECT * FROM patients")
        cte_b = builder.create_cte("filter1", f"SELECT * FROM {cte_a} WHERE active = true", [cte_a])
        cte_c = builder.create_cte("filter2", f"SELECT * FROM {cte_a} WHERE type = 'VIP'", [cte_a])
        cte_d = builder.create_cte("combined", f"SELECT * FROM {cte_b} UNION SELECT * FROM {cte_c}", [cte_b, cte_c])
        
        main_sql = f"SELECT COUNT(*) FROM {cte_d}"
        result = builder.build_final_query(main_sql)
        
        # Verify ordering respects dependencies
        source_pos = result.find("source_1 AS")
        filter1_pos = result.find("filter1_2 AS")
        filter2_pos = result.find("filter2_3 AS")
        combined_pos = result.find("combined_4 AS")
        
        # Source should come first
        assert source_pos < filter1_pos
        assert source_pos < filter2_pos
        
        # Combined should come after both filters
        assert filter1_pos < combined_pos
        assert filter2_pos < combined_pos


class TestCTEBuilderMerging:
    """Test merging functionality between CTEBuilder instances."""
    
    def test_merge_from_empty(self):
        """Test merging from empty CTEBuilder."""
        builder1 = CTEBuilder()
        builder2 = CTEBuilder()
        
        builder1.create_cte("test", "SELECT 1")
        original_count = builder1.get_cte_count()
        
        builder1.merge_from(builder2)
        
        assert builder1.get_cte_count() == original_count  # No change
    
    def test_merge_from_basic(self):
        """Test basic merging of CTEs from another builder."""
        builder1 = CTEBuilder()
        builder2 = CTEBuilder()
        
        cte1 = builder1.create_cte("first", "SELECT 1")
        cte2 = builder2.create_cte("second", "SELECT 2")
        
        builder1.merge_from(builder2)
        
        assert builder1.get_cte_count() == 2
        assert cte1 in builder1.ctes
        assert cte2 in builder1.ctes
        assert builder1.ctes[cte1] == "SELECT 1"
        assert builder1.ctes[cte2] == "SELECT 2"
    
    def test_merge_from_with_dependencies(self):
        """Test merging CTEs that have dependencies."""
        builder1 = CTEBuilder()
        builder2 = CTEBuilder()
        
        # Create CTEs with dependencies in builder2
        base = builder2.create_cte("base", "SELECT id FROM table1")
        derived = builder2.create_cte("derived", f"SELECT * FROM {base}", [base])
        
        builder1.merge_from(builder2)
        
        assert builder1.get_cte_count() == 2
        assert base in builder1.ctes
        assert derived in builder1.ctes
        assert builder1.dependencies[derived] == [base]
    
    def test_merge_from_counter_update(self):
        """Test that counter is properly updated during merge."""
        builder1 = CTEBuilder()
        builder2 = CTEBuilder()
        
        # Create several CTEs to advance counters
        builder1.create_cte("test", "SELECT 1")
        builder1.create_cte("test", "SELECT 2")
        
        builder2.create_cte("other", "SELECT 3")
        builder2.create_cte("other", "SELECT 4")
        builder2.create_cte("other", "SELECT 5")
        
        original_counter = builder1.cte_counter
        builder1.merge_from(builder2)
        
        # Counter should be updated to max of both builders
        assert builder1.cte_counter >= original_counter
        assert builder1.cte_counter >= builder2.cte_counter
    
    def test_merge_from_invalid_type(self):
        """Test error handling when merging from non-CTEBuilder object."""
        builder = CTEBuilder()
        
        with pytest.raises(ValueError, match="Can only merge from another CTEBuilder instance"):
            builder.merge_from("not a CTEBuilder")


class TestCTEBuilderDebuggingAndValidation:
    """Test debugging and validation features."""
    
    def test_debug_info_empty(self):
        """Test debug info for empty builder."""
        builder = CTEBuilder()
        info = builder.debug_info()
        
        assert info['cte_count'] == 0
        assert info['cte_names'] == []
        assert info['has_dependencies'] is False
        assert info['unique_sql_count'] == 0
        assert info['deduplication_ratio'] == 0.0
    
    def test_debug_info_populated(self):
        """Test debug info for populated builder."""
        builder = CTEBuilder()
        
        cte1 = builder.create_cte("filter", "SELECT * FROM patients WHERE active = true")
        cte2 = builder.create_cte("count", f"SELECT COUNT(*) FROM {cte1}", [cte1])
        
        info = builder.debug_info()
        
        assert info['cte_count'] == 2
        assert set(info['cte_names']) == {cte1, cte2}
        assert info['has_dependencies'] is True
        assert cte1 in info['sql_snippets']
        assert cte2 in info['sql_snippets']
        assert info['operations_summary']['filter'] == 1
        assert info['operations_summary']['count'] == 1
    
    def test_debug_info_sql_snippet_truncation(self):
        """Test that long SQL is properly truncated in debug info."""
        builder = CTEBuilder()
        
        long_sql = "SELECT " + ", ".join(f"col_{i}" for i in range(50)) + " FROM table"
        cte_name = builder.create_cte("long", long_sql)
        
        info = builder.debug_info()
        snippet = info['sql_snippets'][cte_name]
        
        assert len(snippet) <= 103  # 100 chars + "..."
        assert snippet.endswith("...")
    
    def test_validate_state_clean(self):
        """Test state validation for clean builder."""
        builder = CTEBuilder()
        
        cte1 = builder.create_cte("base", "SELECT id FROM patients")
        cte2 = builder.create_cte("filtered", f"SELECT * FROM {cte1}", [cte1])
        
        issues = builder.validate_state()
        assert len(issues) == 0
    
    def test_validate_state_finds_issues(self):
        """Test that state validation detects inconsistencies."""
        builder = CTEBuilder()
        
        cte1 = builder.create_cte("test", "SELECT 1")
        
        # Manually corrupt state to test validation
        builder.dependencies["nonexistent"] = []
        builder.dependencies[cte1] = ["missing_dep"]
        
        issues = builder.validate_state()
        
        assert len(issues) >= 2
        assert any("non-existent CTE: nonexistent" in issue for issue in issues)
        assert any("depends on non-existent CTE: missing_dep" in issue for issue in issues)
    
    def test_clear_resets_state(self):
        """Test that clear() resets builder to initial state."""
        builder = CTEBuilder()
        
        # Populate builder
        builder.create_cte("test1", "SELECT 1")
        builder.create_cte("test2", "SELECT 2") 
        
        assert builder.get_cte_count() > 0
        assert builder.cte_counter > 0
        
        # Clear and verify reset
        builder.clear()
        
        assert builder.get_cte_count() == 0
        assert builder.cte_counter == 0
        assert len(builder.dependencies) == 0
        assert len(builder._sql_hashes) == 0
        assert len(builder._creation_order) == 0
        assert len(builder._operation_types) == 0
        assert not builder.has_ctes()


class TestCTEBuilderStringRepresentation:
    """Test string representation methods."""
    
    def test_str_empty(self):
        """Test string representation of empty builder."""
        builder = CTEBuilder()
        assert str(builder) == "CTEBuilder(empty)"
    
    def test_str_populated(self):
        """Test string representation of populated builder."""
        builder = CTEBuilder()
        
        cte1 = builder.create_cte("filter", "SELECT * FROM patients")
        cte2 = builder.create_cte("count", "SELECT COUNT(*) FROM filter_1")
        
        result = str(builder)
        assert "CTEBuilder(2 CTEs:" in result
        assert cte1 in result
        assert cte2 in result
    
    def test_repr(self):
        """Test detailed representation."""
        builder = CTEBuilder()
        
        builder.create_cte("test", "SELECT 1")
        
        result = repr(builder)
        assert "CTEBuilder(ctes=1, dependencies=1, counter=1)" == result


class TestCTEBuilderEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_large_number_of_ctes(self):
        """Test handling of large number of CTEs."""
        builder = CTEBuilder()
        
        # Create many CTEs to test performance and memory usage
        num_ctes = 100
        cte_names = []
        
        for i in range(num_ctes):
            cte_name = builder.create_cte(f"cte_{i}", f"SELECT {i} as value")
            cte_names.append(cte_name)
        
        assert builder.get_cte_count() == num_ctes
        
        # Test that all CTEs can be resolved
        ordered = builder._resolve_dependencies()
        assert len(ordered) == num_ctes
        
        # Test final query building
        main_sql = "SELECT 1"
        result = builder.build_final_query(main_sql)
        assert result.count("WITH") == 1
        assert result.count(" AS ") == num_ctes
    
    def test_very_long_sql(self):
        """Test handling of very long SQL statements."""
        builder = CTEBuilder()
        
        # Create very long SQL
        long_sql = "SELECT " + ", ".join(f"very_long_column_name_{i}" for i in range(1000)) + " FROM table"
        
        cte_name = builder.create_cte("long", long_sql)
        
        assert cte_name in builder.ctes
        assert builder.ctes[cte_name] == long_sql
        
        # Test that it can be included in final query
        main_sql = f"SELECT COUNT(*) FROM {cte_name}"
        result = builder.build_final_query(main_sql)
        assert long_sql in result
    
    def test_complex_dependency_chains(self):
        """Test very complex dependency chains."""
        builder = CTEBuilder()
        
        # Create linear chain: A -> B -> C -> D -> E
        prev_cte = None
        for i in range(5):
            if prev_cte is None:
                cte = builder.create_cte(f"stage_{i}", f"SELECT {i} as stage")
            else:
                cte = builder.create_cte(f"stage_{i}", f"SELECT stage + 1 FROM {prev_cte}", [prev_cte])
            prev_cte = cte
        
        # Verify dependency resolution works
        ordered = builder._resolve_dependencies()
        assert len(ordered) == 5
        
        # Verify they're in correct order
        for i in range(4):
            stage_current = f"stage_{i}_" + str(i + 1)
            stage_next = f"stage_{i+1}_" + str(i + 2)
            
            assert ordered.index(stage_current) < ordered.index(stage_next)
    
    def test_unicode_and_special_characters(self):
        """Test handling of unicode and special characters in SQL."""
        builder = CTEBuilder()
        
        # SQL with unicode and special characters
        unicode_sql = "SELECT 'Héllo Wörld! 🚀' as greeting, 'Special chars: @#$%^&*()' as special"
        
        cte_name = builder.create_cte("unicode", unicode_sql)
        
        assert cte_name in builder.ctes
        assert builder.ctes[cte_name] == unicode_sql
        
        # Test final query building works
        result = builder.build_final_query(f"SELECT * FROM {cte_name}")
        assert unicode_sql in result
        assert "🚀" in result  # Verify unicode is preserved