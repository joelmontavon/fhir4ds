import os
from dataclasses import dataclass, asdict
from typing import Optional

@dataclass
class ParserConfiguration:
    """
    Configuration for FHIRPath parser behavior.

    This data class holds all the settings that can be used to tweak the
    parser's behavior, such as performance optimizations, error handling strategies,
    and feature flags.
    """
    # Performance settings
    enable_metadata_inference: bool = True
    enable_function_validation: bool = True
    max_expression_depth: int = 100

    # Error handling settings
    strict_mode: bool = False
    enable_error_recovery: bool = False
    detailed_error_messages: bool = True

    # Function settings
    enable_custom_functions: bool = True
    function_timeout_ms: int = 1000

    # Development settings
    debug_mode: bool = False
    enable_ast_validation: bool = True

class ConfigurationManager:
    """
    Manages parser configuration from various sources.

    This class is responsible for loading the parser's configuration from
    environment variables and potentially from a configuration file. It provides
    a centralized way to access and update the configuration.
    """
    def __init__(self, initial_config: Optional[dict] = None):
        """
        Initializes the ConfigurationManager.

        Args:
            initial_config: An optional dictionary to override default settings.
        """
        self.config = ParserConfiguration()
        self._load_configuration()
        if initial_config:
            self.update_config(**initial_config)

    def _load_configuration(self):
        """
        Loads configuration from environment variables.
        This method can be extended to load from files (e.g., JSON, YAML).
        """
        # Environment variables for boolean flags
        if os.getenv('FHIRPATH_STRICT_MODE'):
            self.config.strict_mode = os.getenv('FHIRPATH_STRICT_MODE', '').lower() == 'true'
        if os.getenv('FHIRPATH_DEBUG'):
            self.config.debug_mode = os.getenv('FHIRPATH_DEBUG', '').lower() == 'true'

        # Example for an integer setting
        if os.getenv('FHIRPATH_MAX_EXPRESSION_DEPTH'):
            try:
                self.config.max_expression_depth = int(os.getenv('FHIRPATH_MAX_EXPRESSION_DEPTH'))
            except (ValueError, TypeError):
                # Optionally log a warning that the value is invalid
                pass

    def get_config(self) -> ParserConfiguration:
        """Returns the current parser configuration."""
        return self.config

    def update_config(self, **kwargs):
        """
        Updates the configuration at runtime.

        Args:
            **kwargs: Key-value pairs of configuration settings to update.
        """
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
            else:
                # Optionally, raise an error or log a warning for unknown config keys
                pass