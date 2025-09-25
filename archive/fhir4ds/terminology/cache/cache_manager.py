"""
Multi-Tier Terminology Caching System

Implements a three-tier caching strategy:
- Tier 1: In-memory dict cache with LRU eviction (hot cache)
- Tier 2: Database cache using the same database as FHIR resources (warm cache)
- Tier 3: VSAC API calls (cold cache)
"""

import time
import hashlib
import json
import logging
from typing import Dict, Any, Optional, List, Union
from collections import OrderedDict

logger = logging.getLogger(__name__)


class TerminologyCache:
    """
    Multi-tier terminology caching system for VSAC and other terminology services.
    
    Uses the same database connection as FHIR resources for cache persistence,
    supporting both DuckDB and PostgreSQL.
    """
    
    def __init__(self, 
                 memory_capacity: int = 10000,
                 db_connection=None,
                 dialect: str = "duckdb",
                 enable_persistence: bool = True,
                 default_ttl: int = 604800):  # 7 days
        """
        Initialize terminology cache using existing database connection.
        
        Args:
            memory_capacity: Maximum entries in in-memory cache
            db_connection: Database connection (DuckDB or psycopg2/SQLAlchemy)
            dialect: Database dialect ("duckdb" or "postgresql")
            enable_persistence: Whether to enable cache persistence
            default_ttl: Default TTL in seconds (7 days)
        """
        self.memory_capacity = memory_capacity
        self.default_ttl = default_ttl
        self.enable_persistence = enable_persistence
        self.dialect = dialect.lower()
        
        # Tier 1: In-memory dict cache (LRU)
        self.memory_cache = OrderedDict()
        self.memory_ttl = {}
        
        # Tier 2: Database cache using actual database connection
        self.db = db_connection
        self.cache_table_prefix = "terminology_cache"
        
        if self.enable_persistence and self.db:
            self._init_cache_tables()
        
        # Cache statistics
        self.stats = {
            'memory_hits': 0,
            'db_hits': 0, 
            'api_calls': 0,
            'memory_evictions': 0,
            'cache_misses': 0
        }
        
        logger.info(f"Initialized TerminologyCache: memory_capacity={memory_capacity}, "
                   f"dialect={dialect}, enable_persistence={enable_persistence}")
    
    def _init_cache_tables(self):
        """Initialize cache tables with dialect-specific SQL."""
        logger.debug(f"Initializing {self.dialect} cache tables")
        
        try:
            if self.dialect == "duckdb":
                self._init_duckdb_tables()
            elif self.dialect == "postgresql":
                self._init_postgresql_tables()
            else:
                logger.warning(f"Unsupported dialect for cache: {self.dialect}")
                self.enable_persistence = False
                return
            
            logger.debug(f"{self.dialect} cache tables initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize cache tables: {e}")
            self.enable_persistence = False
    
    def _init_duckdb_tables(self):
        """Initialize DuckDB-specific cache tables."""
        # Main valueset cache table
        self.db.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.cache_table_prefix}_valueset (
                cache_key VARCHAR PRIMARY KEY,
                valueset_url VARCHAR NOT NULL,
                version VARCHAR,
                operation VARCHAR NOT NULL,  -- expand, validate, lookup
                parameters JSON,
                result JSON NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ttl_seconds INTEGER DEFAULT 604800,  -- 7 days
                access_count INTEGER DEFAULT 1,
                last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Indexes for performance
        self.db.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.cache_table_prefix}_cache_key 
            ON {self.cache_table_prefix}_valueset(cache_key)
        """)
        
        self.db.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.cache_table_prefix}_valueset_url 
            ON {self.cache_table_prefix}_valueset(valueset_url)
        """)
        
        self.db.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.cache_table_prefix}_operation 
            ON {self.cache_table_prefix}_valueset(operation)
        """)
        
        # Code validation cache table (separate for optimization)
        self.db.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.cache_table_prefix}_code_validation (
                cache_key VARCHAR PRIMARY KEY,
                code VARCHAR NOT NULL,
                system VARCHAR NOT NULL,
                valueset_url VARCHAR,
                is_valid BOOLEAN NOT NULL,
                display VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ttl_seconds INTEGER DEFAULT 86400,  -- 1 day
                last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.db.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.cache_table_prefix}_code_validation 
            ON {self.cache_table_prefix}_code_validation(code, system)
        """)
    
    def _init_postgresql_tables(self):
        """Initialize PostgreSQL-specific cache tables."""
        # Main valueset cache table
        self.db.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.cache_table_prefix}_valueset (
                cache_key VARCHAR(32) PRIMARY KEY,
                valueset_url VARCHAR(500) NOT NULL,
                version VARCHAR(100),
                operation VARCHAR(20) NOT NULL,  -- expand, validate, lookup
                parameters JSONB,
                result JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ttl_seconds INTEGER DEFAULT 604800,  -- 7 days
                access_count INTEGER DEFAULT 1,
                last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Indexes for performance
        self.db.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.cache_table_prefix}_cache_key 
            ON {self.cache_table_prefix}_valueset(cache_key)
        """)
        
        self.db.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.cache_table_prefix}_valueset_url 
            ON {self.cache_table_prefix}_valueset(valueset_url)
        """)
        
        self.db.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.cache_table_prefix}_operation 
            ON {self.cache_table_prefix}_valueset(operation)
        """)
        
        # Code validation cache table (separate for optimization)
        self.db.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.cache_table_prefix}_code_validation (
                cache_key VARCHAR(32) PRIMARY KEY,
                code VARCHAR(50) NOT NULL,
                system VARCHAR(500) NOT NULL,
                valueset_url VARCHAR(500),
                is_valid BOOLEAN NOT NULL,
                display VARCHAR(500),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ttl_seconds INTEGER DEFAULT 86400,  -- 1 day
                last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.db.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.cache_table_prefix}_code_validation 
            ON {self.cache_table_prefix}_code_validation(code, system)
        """)
        
        # Commit for PostgreSQL
        if hasattr(self.db, 'commit'):
            self.db.commit()
    
    def _generate_cache_key(self, operation: str, **kwargs) -> str:
        """
        Generate consistent cache key for any operation.
        
        Args:
            operation: Operation type (expand, validate, lookup, etc.)
            **kwargs: Operation parameters
            
        Returns:
            32-character hex cache key
        """
        # Sort parameters for consistent hashing
        sorted_params = sorted(kwargs.items())
        param_str = json.dumps(sorted_params, sort_keys=True)
        combined = f"{operation}:{param_str}"
        return hashlib.sha256(combined.encode()).hexdigest()[:32]
    
    def get_valueset_expansion(self, valueset_url: str, 
                             version: str = None, 
                             parameters: Dict = None) -> Optional[Dict[str, Any]]:
        """
        Get cached valueset expansion with tier fallback.
        
        Args:
            valueset_url: ValueSet canonical URL or OID
            version: Specific version (optional)
            parameters: Additional expansion parameters
            
        Returns:
            Cached FHIR ValueSet expansion or None if not cached
        """
        cache_key = self._generate_cache_key(
            "expand", 
            url=valueset_url, 
            version=version,
            params=parameters or {}
        )
        
        logger.debug(f"Checking cache for valueset expansion: {valueset_url}")
        
        # Tier 1: Memory cache check
        result = self._get_from_memory(cache_key)
        if result is not None:
            self.stats['memory_hits'] += 1
            logger.debug(f"Memory cache hit for valueset: {valueset_url}")
            return result
        
        # Tier 2: Database cache check  
        if self.enable_persistence:
            result = self._get_from_db(cache_key, f"{self.cache_table_prefix}_valueset")
            if result is not None:
                self.stats['db_hits'] += 1
                logger.debug(f"DB cache hit for valueset: {valueset_url}")
                # Promote to memory cache
                self._set_in_memory(cache_key, result, ttl=3600)  # 1 hour in memory
                return result
        
        # Cache miss
        self.stats['cache_misses'] += 1
        logger.debug(f"Cache miss for valueset: {valueset_url}")
        return None
    
    def cache_valueset_expansion(self, valueset_url: str, 
                               result: Dict[str, Any],
                               version: str = None,
                               parameters: Dict = None,
                               ttl: int = None):
        """
        Cache valueset expansion in both memory and DB tiers.
        
        Args:
            valueset_url: ValueSet canonical URL or OID
            result: FHIR ValueSet expansion result
            version: Specific version (optional)
            parameters: Additional expansion parameters
            ttl: Time-to-live in seconds (uses default if None)
        """
        if ttl is None:
            ttl = self.default_ttl
            
        cache_key = self._generate_cache_key(
            "expand",
            url=valueset_url,
            version=version, 
            params=parameters or {}
        )
        
        logger.debug(f"Caching valueset expansion: {valueset_url} (TTL: {ttl}s)")
        
        # Store in memory cache (shorter TTL for memory)
        memory_ttl = min(ttl, 3600)  # Max 1 hour in memory
        self._set_in_memory(cache_key, result, ttl=memory_ttl)
        
        # Store in DB cache
        if self.enable_persistence:
            self._set_in_db(cache_key, {
                'valueset_url': valueset_url,
                'version': version,
                'operation': 'expand',
                'parameters': json.dumps(parameters or {}),
                'result': json.dumps(result),
                'ttl_seconds': ttl
            }, f"{self.cache_table_prefix}_valueset")
    
    def get_code_validation(self, code: str, system: str, 
                           valueset_url: str = None) -> Optional[bool]:
        """
        Get cached code validation result.
        
        Args:
            code: Code to validate
            system: Code system URL
            valueset_url: ValueSet URL (optional)
            
        Returns:
            Validation result (True/False) or None if not cached
        """
        cache_key = self._generate_cache_key(
            "validate",
            code=code,
            system=system,
            valueset_url=valueset_url or ""
        )
        
        # Try memory cache first
        result = self._get_from_memory(cache_key)
        if result is not None:
            self.stats['memory_hits'] += 1
            return result.get('is_valid')
        
        # Try DB cache
        if self.enable_persistence:
            try:
                table_name = f"{self.cache_table_prefix}_code_validation"
                
                if self.dialect == "duckdb":
                    cursor = self.db.execute(f"""
                        SELECT is_valid, created_at, ttl_seconds 
                        FROM {table_name} 
                        WHERE cache_key = ? 
                        AND (created_at + INTERVAL (ttl_seconds) SECOND) > CURRENT_TIMESTAMP
                    """, [cache_key])
                elif self.dialect == "postgresql":
                    cursor = self.db.execute(f"""
                        SELECT is_valid, created_at, ttl_seconds 
                        FROM {table_name} 
                        WHERE cache_key = %s 
                        AND (created_at + INTERVAL '1 second' * ttl_seconds) > CURRENT_TIMESTAMP
                    """, [cache_key])
                
                row = cursor.fetchone()
                if row:
                    self.stats['db_hits'] += 1
                    # Update access tracking
                    if self.dialect == "duckdb":
                        self.db.execute(f"""
                            UPDATE {table_name} 
                            SET last_accessed = CURRENT_TIMESTAMP
                            WHERE cache_key = ?
                        """, [cache_key])
                    elif self.dialect == "postgresql":
                        self.db.execute(f"""
                            UPDATE {table_name} 
                            SET last_accessed = CURRENT_TIMESTAMP
                            WHERE cache_key = %s
                        """, [cache_key])
                        if hasattr(self.db, 'commit'):
                            self.db.commit()
                    
                    # Promote to memory
                    validation_result = {'is_valid': row[0]}
                    self._set_in_memory(cache_key, validation_result, ttl=3600)
                    return row[0]
            except Exception as e:
                logger.warning(f"Error accessing code validation cache: {e}")
        
        self.stats['cache_misses'] += 1
        return None
    
    def cache_code_validation(self, code: str, system: str, is_valid: bool,
                            valueset_url: str = None, display: str = None,
                            ttl: int = 86400):  # 1 day default
        """
        Cache code validation result.
        
        Args:
            code: Code that was validated
            system: Code system URL
            is_valid: Validation result
            valueset_url: ValueSet URL (optional)
            display: Code display name (optional)
            ttl: Time-to-live in seconds
        """
        cache_key = self._generate_cache_key(
            "validate",
            code=code,
            system=system,
            valueset_url=valueset_url or ""
        )
        
        # Store in memory
        validation_result = {'is_valid': is_valid}
        self._set_in_memory(cache_key, validation_result, ttl=min(ttl, 3600))
        
        # Store in DB
        if self.enable_persistence:
            try:
                table_name = f"{self.cache_table_prefix}_code_validation"
                
                if self.dialect == "duckdb":
                    self.db.execute(f"""
                        INSERT OR REPLACE INTO {table_name} 
                        (cache_key, code, system, valueset_url, is_valid, display, ttl_seconds)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, [cache_key, code, system, valueset_url, is_valid, display, ttl])
                elif self.dialect == "postgresql":
                    self.db.execute(f"""
                        INSERT INTO {table_name} 
                        (cache_key, code, system, valueset_url, is_valid, display, ttl_seconds)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (cache_key) DO UPDATE SET
                            is_valid = EXCLUDED.is_valid,
                            display = EXCLUDED.display,
                            last_accessed = CURRENT_TIMESTAMP
                    """, [cache_key, code, system, valueset_url, is_valid, display, ttl])
                    if hasattr(self.db, 'commit'):
                        self.db.commit()
            except Exception as e:
                logger.warning(f"Error caching code validation: {e}")
    
    def _get_from_memory(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """
        Get from Tier 1 memory cache with TTL check.
        
        Args:
            cache_key: Cache key to lookup
            
        Returns:
            Cached result or None if not found/expired
        """
        if cache_key in self.memory_cache:
            # Check TTL
            if cache_key in self.memory_ttl:
                if time.time() > self.memory_ttl[cache_key]:
                    # Expired - remove
                    del self.memory_cache[cache_key]
                    del self.memory_ttl[cache_key]
                    return None
            
            # Move to end (LRU)
            result = self.memory_cache.pop(cache_key)
            self.memory_cache[cache_key] = result
            return result
        return None
    
    def _set_in_memory(self, cache_key: str, result: Dict[str, Any], ttl: int):
        """
        Set in Tier 1 memory cache with LRU eviction.
        
        Args:
            cache_key: Cache key
            result: Result to cache
            ttl: Time-to-live in seconds
        """
        # Remove oldest if at capacity
        while len(self.memory_cache) >= self.memory_capacity:
            oldest_key = next(iter(self.memory_cache))
            del self.memory_cache[oldest_key]
            self.memory_ttl.pop(oldest_key, None)
            self.stats['memory_evictions'] += 1
        
        self.memory_cache[cache_key] = result
        self.memory_ttl[cache_key] = time.time() + ttl
    
    def _get_from_db(self, cache_key: str, table: str) -> Optional[Dict[str, Any]]:
        """
        Get from Tier 2 database cache with TTL check.
        
        Args:
            cache_key: Cache key to lookup
            table: Table name to query
            
        Returns:
            Cached result or None if not found/expired
        """
        if not self.enable_persistence:
            return None
            
        try:
            if self.dialect == "duckdb":
                cursor = self.db.execute(f"""
                    SELECT result, created_at, ttl_seconds 
                    FROM {table} 
                    WHERE cache_key = ? 
                    AND (created_at + INTERVAL (ttl_seconds) SECOND) > CURRENT_TIMESTAMP
                """, [cache_key])
            elif self.dialect == "postgresql":
                cursor = self.db.execute(f"""
                    SELECT result, created_at, ttl_seconds 
                    FROM {table} 
                    WHERE cache_key = %s 
                    AND (created_at + INTERVAL '1 second' * ttl_seconds) > CURRENT_TIMESTAMP
                """, [cache_key])
            
            row = cursor.fetchone()
            if row:
                # Update access tracking
                if self.dialect == "duckdb":
                    self.db.execute(f"""
                        UPDATE {table} 
                        SET access_count = access_count + 1,
                            last_accessed = CURRENT_TIMESTAMP
                        WHERE cache_key = ?
                    """, [cache_key])
                elif self.dialect == "postgresql":
                    self.db.execute(f"""
                        UPDATE {table} 
                        SET access_count = access_count + 1,
                            last_accessed = CURRENT_TIMESTAMP
                        WHERE cache_key = %s
                    """, [cache_key])
                    if hasattr(self.db, 'commit'):
                        self.db.commit()
                
                return json.loads(row[0])
        except Exception as e:
            logger.warning(f"Cache DB read error: {e}")
        
        return None
    
    def _set_in_db(self, cache_key: str, data: Dict[str, Any], table: str):
        """
        Set in Tier 2 database cache.
        
        Args:
            cache_key: Cache key
            data: Data to cache
            table: Table name to insert into
        """
        if not self.enable_persistence:
            return
            
        try:
            if self.dialect == "duckdb":
                self.db.execute(f"""
                    INSERT OR REPLACE INTO {table} 
                    (cache_key, valueset_url, version, operation, parameters, result, ttl_seconds)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, [
                    cache_key,
                    data['valueset_url'],
                    data.get('version'),
                    data['operation'],
                    data['parameters'],
                    data['result'],
                    data['ttl_seconds']
                ])
            elif self.dialect == "postgresql":
                self.db.execute(f"""
                    INSERT INTO {table} 
                    (cache_key, valueset_url, version, operation, parameters, result, ttl_seconds)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (cache_key) DO UPDATE SET
                        result = EXCLUDED.result,
                        access_count = {table}.access_count + 1,
                        last_accessed = CURRENT_TIMESTAMP
                """, [
                    cache_key,
                    data['valueset_url'],
                    data.get('version'),
                    data['operation'],
                    data['parameters'],
                    data['result'],
                    data['ttl_seconds']
                ])
                if hasattr(self.db, 'commit'):
                    self.db.commit()
        except Exception as e:
            logger.warning(f"Cache DB write error: {e}")
    
    def clear_expired(self):
        """Clear expired entries from both memory and DB caches."""
        logger.info("Clearing expired cache entries")
        
        # Clear memory cache
        current_time = time.time()
        expired_keys = [
            key for key, expiry in self.memory_ttl.items() 
            if current_time > expiry
        ]
        
        for key in expired_keys:
            self.memory_cache.pop(key, None)
            self.memory_ttl.pop(key, None)
        
        logger.debug(f"Cleared {len(expired_keys)} expired memory cache entries")
        
        # Clear DB caches
        if self.enable_persistence:
            try:
                valueset_table = f"{self.cache_table_prefix}_valueset"
                validation_table = f"{self.cache_table_prefix}_code_validation"
                
                if self.dialect == "duckdb":
                    valueset_deleted = self.db.execute(f"""
                        DELETE FROM {valueset_table} 
                        WHERE (created_at + INTERVAL (ttl_seconds) SECOND) < CURRENT_TIMESTAMP
                    """).fetchall()
                    
                    validation_deleted = self.db.execute(f"""
                        DELETE FROM {validation_table} 
                        WHERE (created_at + INTERVAL (ttl_seconds) SECOND) < CURRENT_TIMESTAMP
                    """).fetchall()
                elif self.dialect == "postgresql":
                    cursor = self.db.execute(f"""
                        DELETE FROM {valueset_table} 
                        WHERE (created_at + INTERVAL '1 second' * ttl_seconds) < CURRENT_TIMESTAMP
                    """)
                    valueset_deleted_count = cursor.rowcount if hasattr(cursor, 'rowcount') else 0
                    
                    cursor = self.db.execute(f"""
                        DELETE FROM {validation_table} 
                        WHERE (created_at + INTERVAL '1 second' * ttl_seconds) < CURRENT_TIMESTAMP
                    """)
                    validation_deleted_count = cursor.rowcount if hasattr(cursor, 'rowcount') else 0
                    
                    if hasattr(self.db, 'commit'):
                        self.db.commit()
                    
                    valueset_deleted = [None] * valueset_deleted_count  # Mock for logging
                    validation_deleted = [None] * validation_deleted_count
                
                logger.debug(f"Cleared expired DB cache entries: "
                            f"valueset={len(valueset_deleted)}, validation={len(validation_deleted)}")
            except Exception as e:
                logger.warning(f"Error clearing expired DB cache entries: {e}")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive cache statistics.
        
        Returns:
            Dictionary with detailed cache statistics
        """
        # Get DB stats
        db_stats = (0, 0, 0, 0)
        validation_stats = (0,)
        
        if self.enable_persistence:
            try:
                valueset_table = f"{self.cache_table_prefix}_valueset"
                validation_table = f"{self.cache_table_prefix}_code_validation"
                
                db_stats = self.db.execute(f"""
                    SELECT 
                        COUNT(*) as total_entries,
                        SUM(CASE WHEN operation = 'expand' THEN 1 ELSE 0 END) as expansions,
                        SUM(access_count) as total_accesses,
                        AVG(access_count) as avg_accesses
                    FROM {valueset_table}
                """).fetchone()
                
                validation_stats = self.db.execute(f"""
                    SELECT COUNT(*) as validation_entries
                    FROM {validation_table}
                """).fetchone()
            except Exception as e:
                logger.warning(f"Error getting DB stats: {e}")
                db_stats = (0, 0, 0, 0)
                validation_stats = (0,)
        
        total_requests = sum([
            self.stats['memory_hits'],
            self.stats['db_hits'], 
            self.stats['cache_misses']
        ])
        
        hit_rate = 0.0
        if total_requests > 0:
            hit_rate = (self.stats['memory_hits'] + self.stats['db_hits']) / total_requests
        
        return {
            **self.stats,
            'memory_cache_size': len(self.memory_cache),
            'memory_capacity': self.memory_capacity,
            'db_cache_entries': db_stats[0] if db_stats else 0,
            'cached_expansions': db_stats[1] if db_stats else 0,
            'cached_validations': validation_stats[0] if validation_stats else 0,
            'total_db_accesses': db_stats[2] if db_stats else 0,
            'avg_access_count': db_stats[3] if db_stats else 0,
            'total_requests': total_requests,
            'hit_rate': hit_rate,
            'cache_efficiency': hit_rate * 100
        }
    
    def clear_all_caches(self):
        """Clear all cache data (memory and DB)."""
        logger.warning("Clearing all cache data")
        
        # Clear memory
        self.memory_cache.clear()
        self.memory_ttl.clear()
        
        # Clear DB
        if self.enable_persistence:
            try:
                valueset_table = f"{self.cache_table_prefix}_valueset"
                validation_table = f"{self.cache_table_prefix}_code_validation"
                
                self.db.execute(f"DELETE FROM {valueset_table}")
                self.db.execute(f"DELETE FROM {validation_table}")
                
                if self.dialect == "postgresql" and hasattr(self.db, 'commit'):
                    self.db.commit()
            except Exception as e:
                logger.error(f"Error clearing DB caches: {e}")
        
        # Reset stats
        self.stats = {
            'memory_hits': 0,
            'db_hits': 0, 
            'api_calls': 0,
            'memory_evictions': 0,
            'cache_misses': 0
        }
    
    def close(self):
        """Close cache resources (for compatibility - actual DB connection managed elsewhere)."""
        # Note: We don't close the DB connection since it's shared with the main application
        # The application is responsible for managing the database connection lifecycle
        logger.debug("TerminologyCache close() called - database connection managed by application")