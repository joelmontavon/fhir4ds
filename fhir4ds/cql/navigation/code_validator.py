"""
Individual Code Validation Framework

Provides real-time validation of clinical codes against authoritative terminology sources.
Supports VSAC (Value Set Authority Center), FHIR terminology servers, and custom validators.

Phase 4: Navigation Enhancement - Individual Code Validation
"""

import os
import time
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple
from enum import Enum
import threading
from urllib.parse import urljoin, quote

# Optional imports with graceful degradation
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    requests = None

try:
    from cachetools import TTLCache
    CACHETOOLS_AVAILABLE = True
except ImportError:
    CACHETOOLS_AVAILABLE = False
    # Fallback simple cache implementation
    class TTLCache:
        def __init__(self, maxsize, ttl):
            self.maxsize = maxsize
            self.ttl = ttl
            self._cache = {}
            self._timestamps = {}
        
        def get(self, key, default=None):
            if key in self._cache:
                if time.time() - self._timestamps[key] < self.ttl:
                    return self._cache[key]
                else:
                    del self._cache[key]
                    del self._timestamps[key]
            return default
        
        def __setitem__(self, key, value):
            if len(self._cache) >= self.maxsize:
                # Simple eviction: remove oldest
                oldest_key = min(self._timestamps.keys(), key=lambda k: self._timestamps[k])
                del self._cache[oldest_key]
                del self._timestamps[oldest_key]
            self._cache[key] = value
            self._timestamps[key] = time.time()
        
        def __getitem__(self, key):
            return self.get(key)
        
        def __contains__(self, key):
            return self.get(key) is not None
        
        def clear(self):
            self._cache.clear()
            self._timestamps.clear()
        
        def __len__(self):
            # Clean expired items first
            current_time = time.time()
            expired_keys = [k for k, t in self._timestamps.items() if current_time - t >= self.ttl]
            for k in expired_keys:
                self._cache.pop(k, None)
                self._timestamps.pop(k, None)
            return len(self._cache)

logger = logging.getLogger(__name__)


class ValidationResult(Enum):
    """Code validation result status."""
    VALID = "valid"
    INVALID = "invalid"
    UNKNOWN = "unknown"  # System/network error
    NOT_FOUND = "not_found"  # Code system not found


@dataclass
class CodeValidationResult:
    """Result of individual code validation."""
    code: str
    system: str
    result: ValidationResult
    display: Optional[str] = None
    message: Optional[str] = None
    issues: List[str] = field(default_factory=list)
    response_time_ms: Optional[float] = None
    value_set_url: Optional[str] = None
    cached: bool = False


class RateLimiter:
    """Thread-safe sliding window rate limiter."""
    
    def __init__(self, max_calls: int, time_window: float = 1.0):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls = []
        self.lock = threading.Lock()
    
    def acquire(self, block: bool = True, timeout: Optional[float] = None) -> bool:
        """Acquire permission to make a request."""
        with self.lock:
            now = time.time()
            # Remove old calls outside the time window
            self.calls = [call_time for call_time in self.calls if now - call_time < self.time_window]
            
            if len(self.calls) < self.max_calls:
                self.calls.append(now)
                return True
            elif block:
                # Calculate wait time
                oldest_call = min(self.calls)
                wait_time = self.time_window - (now - oldest_call)
                if timeout is None or wait_time <= timeout:
                    time.sleep(wait_time)
                    return self.acquire(block=False)
                return False
            else:
                return False


class ValidationCache:
    """TTL cache for code validation results with thread safety."""
    
    def __init__(self, maxsize: int = 10000, ttl_hours: float = 12.0):
        self.cache = TTLCache(maxsize=maxsize, ttl=ttl_hours * 3600)
        self.lock = threading.Lock()
        
    def get(self, key: str) -> Optional[CodeValidationResult]:
        """Get cached validation result."""
        with self.lock:
            result = self.cache.get(key)
            if result:
                # Mark as cached when retrieved
                result.cached = True
            return result
    
    def put(self, key: str, result: CodeValidationResult) -> None:
        """Cache validation result."""
        with self.lock:
            self.cache[key] = result
    
    def clear(self) -> None:
        """Clear all cached results."""
        with self.lock:
            self.cache.clear()
    
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self.lock:
            return {
                'size': len(self.cache),
                'maxsize': self.cache.maxsize,
                'ttl_seconds': self.cache.ttl,
                'hit_rate': getattr(self.cache, 'hits', 0) / max(getattr(self.cache, 'hits', 0) + getattr(self.cache, 'misses', 0), 1)
            }


class CodeValidator(ABC):
    """Abstract base class for code validation implementations."""
    
    def __init__(self, cache_ttl_hours: float = 12.0, cache_maxsize: int = 10000):
        self.cache = ValidationCache(maxsize=cache_maxsize, ttl_hours=cache_ttl_hours)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def _cache_key(self, code: str, system: str, value_set_url: Optional[str] = None) -> str:
        """Generate cache key for code validation."""
        if value_set_url:
            return f"{system}|{code}|{value_set_url}"
        return f"{system}|{code}"
    
    @abstractmethod
    def _validate_code_api(self, code: str, system: str, value_set_url: Optional[str] = None) -> CodeValidationResult:
        """Perform actual API validation (implemented by subclasses)."""
        pass
    
    def validate_code(self, code: str, system: str, value_set_url: Optional[str] = None) -> CodeValidationResult:
        """
        Validate individual code against system/value set with caching.
        
        Args:
            code: The code to validate
            system: Code system URI (e.g., http://loinc.org)
            value_set_url: Optional value set URL for context-specific validation
            
        Returns:
            CodeValidationResult with validation status and details
        """
        # Input validation
        if not code or not system:
            return CodeValidationResult(
                code=code,
                system=system,
                result=ValidationResult.INVALID,
                message="Code and system are required",
                value_set_url=value_set_url
            )
        
        # Check cache first
        cache_key = self._cache_key(code, system, value_set_url)
        cached_result = self.cache.get(cache_key)
        if cached_result:
            self.logger.debug(f"Cache hit for {cache_key}")
            return cached_result
        
        # Perform API validation
        try:
            result = self._validate_code_api(code, system, value_set_url)
            # Cache successful results and explicit invalid results
            if result.result in [ValidationResult.VALID, ValidationResult.INVALID]:
                self.cache.put(cache_key, result)
            return result
        except Exception as e:
            self.logger.error(f"Validation error for {cache_key}: {e}")
            return CodeValidationResult(
                code=code,
                system=system,
                result=ValidationResult.UNKNOWN,
                message=f"Validation error: {str(e)}",
                value_set_url=value_set_url
            )
    
    def validate_codes_batch(self, codes: List[Tuple[str, str, Optional[str]]]) -> List[CodeValidationResult]:
        """
        Batch validate multiple codes for efficiency.
        
        Args:
            codes: List of (code, system, value_set_url) tuples
            
        Returns:
            List of CodeValidationResult objects
        """
        results = []
        for code, system, value_set_url in codes:
            result = self.validate_code(code, system, value_set_url)
            results.append(result)
        return results


class VSACValidator(CodeValidator):
    """VSAC (Value Set Authority Center) code validator using FHIR Terminology Service."""
    
    def __init__(self, umls_api_key: Optional[str] = None, use_production: bool = True, 
                 rate_limit_per_second: int = 20, **kwargs):
        super().__init__(**kwargs)
        
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is required for VSAC validation. Install with: pip install requests")
        
        self.umls_api_key = umls_api_key or os.getenv('VSAC_API_KEY')
        if not self.umls_api_key:
            raise ValueError("UMLS API key is required for VSAC validation. Set VSAC_API_KEY environment variable.")
        
        self.base_url = "https://cts.nlm.nih.gov/fhir/" if use_production else "https://uat-cts.nlm.nih.gov/fhir/"
        self.rate_limiter = RateLimiter(max_calls=rate_limit_per_second, time_window=1.0)
        
        # Configure requests session
        self.session = requests.Session()
        self.session.auth = ('apikey', self.umls_api_key)
        self.session.headers.update({
            'Accept': 'application/fhir+json',
            'User-Agent': 'fhir4ds-code-validator/1.0'
        })
        
        self.timeout = float(os.getenv('VALIDATION_TIMEOUT_SECONDS', '30'))
        self.max_retries = int(os.getenv('VALIDATION_MAX_RETRIES', '3'))
        self.backoff_factor = float(os.getenv('VALIDATION_BACKOFF_FACTOR', '2.0'))
        
        self.logger.info(f"Initialized VSAC validator with base URL: {self.base_url}")
    
    def _validate_code_api(self, code: str, system: str, value_set_url: Optional[str] = None) -> CodeValidationResult:
        """Validate code using VSAC FHIR Terminology Service API."""
        start_time = time.time()
        
        # Construct validation URL
        if value_set_url:
            # Extract OID from value set URL if needed
            if 'urn:oid:' in value_set_url:
                oid = value_set_url.replace('urn:oid:', '')
                validate_url = f"{self.base_url}ValueSet/{oid}/$validate-code"
            else:
                validate_url = f"{self.base_url}ValueSet/$validate-code"
        else:
            validate_url = f"{self.base_url}CodeSystem/$validate-code"
        
        params = {
            'system': system,
            'code': code
        }
        
        if value_set_url and 'urn:oid:' not in value_set_url:
            params['url'] = value_set_url
        
        # Apply rate limiting
        if not self.rate_limiter.acquire(timeout=self.timeout):
            return CodeValidationResult(
                code=code,
                system=system,
                result=ValidationResult.UNKNOWN,
                message="Rate limit exceeded",
                value_set_url=value_set_url,
                response_time_ms=(time.time() - start_time) * 1000
            )
        
        # Perform validation with retries
        last_exception = None
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(validate_url, params=params, timeout=self.timeout)
                response_time_ms = (time.time() - start_time) * 1000
                
                if response.status_code == 200:
                    return self._parse_validation_response(response.json(), code, system, value_set_url, response_time_ms)
                elif response.status_code == 404:
                    return CodeValidationResult(
                        code=code,
                        system=system,
                        result=ValidationResult.NOT_FOUND,
                        message="Code system or value set not found",
                        value_set_url=value_set_url,
                        response_time_ms=response_time_ms
                    )
                elif response.status_code == 429:  # Rate limited
                    if attempt < self.max_retries - 1:
                        wait_time = self.backoff_factor ** attempt
                        self.logger.warning(f"Rate limited, waiting {wait_time}s before retry {attempt + 1}")
                        time.sleep(wait_time)
                        continue
                    else:
                        return CodeValidationResult(
                            code=code,
                            system=system,
                            result=ValidationResult.UNKNOWN,
                            message="Rate limit exceeded after retries",
                            value_set_url=value_set_url,
                            response_time_ms=response_time_ms
                        )
                else:
                    response.raise_for_status()
                    
            except requests.RequestException as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    wait_time = self.backoff_factor ** attempt
                    self.logger.warning(f"Request failed: {e}, retrying in {wait_time}s")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"Validation failed after {self.max_retries} attempts: {e}")
        
        return CodeValidationResult(
            code=code,
            system=system,
            result=ValidationResult.UNKNOWN,
            message=f"Request failed: {str(last_exception)}",
            value_set_url=value_set_url,
            response_time_ms=(time.time() - start_time) * 1000
        )
    
    def _parse_validation_response(self, response_data: Dict[str, Any], code: str, system: str, 
                                   value_set_url: Optional[str], response_time_ms: float) -> CodeValidationResult:
        """Parse FHIR Parameters resource from validation response."""
        try:
            # Extract parameters from FHIR Parameters resource
            parameters = response_data.get('parameter', [])
            
            result = ValidationResult.INVALID
            display = None
            message = None
            issues = []
            
            for param in parameters:
                name = param.get('name')
                if name == 'result':
                    result = ValidationResult.VALID if param.get('valueBoolean') else ValidationResult.INVALID
                elif name == 'display':
                    display = param.get('valueString')
                elif name == 'message':
                    message = param.get('valueString')
                elif name == 'issues':
                    # Handle issues array
                    issues_param = param.get('resource', {}).get('issue', [])
                    for issue in issues_param:
                        if issue.get('details', {}).get('text'):
                            issues.append(issue['details']['text'])
            
            return CodeValidationResult(
                code=code,
                system=system,
                result=result,
                display=display,
                message=message,
                issues=issues,
                value_set_url=value_set_url,
                response_time_ms=response_time_ms
            )
            
        except Exception as e:
            self.logger.error(f"Failed to parse validation response: {e}")
            return CodeValidationResult(
                code=code,
                system=system,
                result=ValidationResult.UNKNOWN,
                message=f"Failed to parse response: {str(e)}",
                value_set_url=value_set_url,
                response_time_ms=response_time_ms
            )


class FHIRTerminologyValidator(CodeValidator):
    """Generic FHIR R4 terminology server validator."""
    
    def __init__(self, base_url: str, auth_token: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is required for FHIR validation. Install with: pip install requests")
        
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        
        if auth_token:
            self.session.headers.update({'Authorization': f'Bearer {auth_token}'})
        
        self.session.headers.update({
            'Accept': 'application/fhir+json',
            'User-Agent': 'fhir4ds-code-validator/1.0'
        })
        
        self.timeout = 30.0
        self.logger.info(f"Initialized FHIR terminology validator with base URL: {base_url}")
    
    def _validate_code_api(self, code: str, system: str, value_set_url: Optional[str] = None) -> CodeValidationResult:
        """Validate code using generic FHIR terminology server."""
        start_time = time.time()
        
        if value_set_url:
            validate_url = f"{self.base_url}/ValueSet/$validate-code"
            params = {'url': value_set_url, 'system': system, 'code': code}
        else:
            validate_url = f"{self.base_url}/CodeSystem/$validate-code"
            params = {'system': system, 'code': code}
        
        try:
            response = self.session.get(validate_url, params=params, timeout=self.timeout)
            response_time_ms = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                return self._parse_validation_response(response.json(), code, system, value_set_url, response_time_ms)
            elif response.status_code == 404:
                return CodeValidationResult(
                    code=code,
                    system=system,
                    result=ValidationResult.NOT_FOUND,
                    message="Code system or value set not found",
                    value_set_url=value_set_url,
                    response_time_ms=response_time_ms
                )
            else:
                response.raise_for_status()
                
        except requests.RequestException as e:
            self.logger.error(f"FHIR validation request failed: {e}")
            return CodeValidationResult(
                code=code,
                system=system,
                result=ValidationResult.UNKNOWN,
                message=f"Request failed: {str(e)}",
                value_set_url=value_set_url,
                response_time_ms=(time.time() - start_time) * 1000
            )
    
    def _parse_validation_response(self, response_data: Dict[str, Any], code: str, system: str,
                                   value_set_url: Optional[str], response_time_ms: float) -> CodeValidationResult:
        """Parse FHIR Parameters resource from validation response."""
        # Reuse VSAC parsing logic as it's standard FHIR Parameters format
        vsac_validator = VSACValidator.__new__(VSACValidator)
        return vsac_validator._parse_validation_response(response_data, code, system, value_set_url, response_time_ms)


# Factory function for convenient validator creation
def create_validator(validator_type: str = "vsac", **kwargs) -> CodeValidator:
    """
    Create a code validator instance.
    
    Args:
        validator_type: Type of validator ('vsac' or 'fhir')
        **kwargs: Additional arguments for validator constructor
        
    Returns:
        CodeValidator instance
    """
    if not REQUESTS_AVAILABLE:
        raise RuntimeError("requests library is required for code validation. Install with: pip install requests")
    
    if validator_type.lower() == "vsac":
        return VSACValidator(**kwargs)
    elif validator_type.lower() == "fhir":
        if 'base_url' not in kwargs:
            raise ValueError("base_url is required for FHIR terminology validator")
        return FHIRTerminologyValidator(**kwargs)
    else:
        raise ValueError(f"Unknown validator type: {validator_type}")