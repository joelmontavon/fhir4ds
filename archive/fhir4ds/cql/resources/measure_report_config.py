"""
MeasureReport Configuration System

This module provides configuration classes for controlling MeasureReport generation
from CQL execution results. Supports individual, subject-list, and bundle output formats.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import datetime, date
from enum import Enum

logger = logging.getLogger(__name__)

class ReportType(Enum):
    """FHIR MeasureReport types."""
    INDIVIDUAL = "individual"
    SUBJECT_LIST = "subject-list"
    SUMMARY = "summary"
    DATA_COLLECTION = "data-collection"

class OutputFormat(Enum):
    """Output format options."""
    MEASUREREPORT = "measurereport"
    BUNDLE = "bundle"
    RAW = "raw"

class ReportStatus(Enum):
    """FHIR MeasureReport status values."""
    COMPLETE = "complete"
    PENDING = "pending"
    ERROR = "error"

class ImprovementNotation(Enum):
    """FHIR improvement notation values."""
    INCREASE = "increase"
    DECREASE = "decrease"

@dataclass
class MeasureReportConfig:
    """
    Configuration for MeasureReport generation from CQL results.
    
    This class controls how CQL define statement results are transformed
    into FHIR-compliant MeasureReport resources.
    """
    
    # Report types to generate
    report_types: List[str] = field(default_factory=lambda: [ReportType.SUBJECT_LIST.value])
    
    # Output format
    output_format: str = OutputFormat.MEASUREREPORT.value
    
    # Include evaluated resources in report
    include_evaluated_resources: bool = True
    
    # Reference to the Measure resource
    measure_reference: Optional[str] = None
    
    # Reporting period (start/end dates)
    reporting_period: Optional[Dict[str, str]] = None
    
    # Report status
    status: str = ReportStatus.COMPLETE.value
    
    # Improvement notation
    improvement_notation: Optional[str] = None
    
    # Reporter organization/system
    reporter: Optional[str] = None
    
    # Custom population mapping (define name -> population type)
    population_mapping: Optional[Dict[str, str]] = None
    
    # Bundle type for bundle output
    bundle_type: str = "collection"
    
    # Generate resource IDs automatically
    auto_generate_ids: bool = True
    
    # Include individual patient results in subject-list reports
    include_subject_details: bool = False
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        self.validate()
    
    def validate(self) -> None:
        """
        Validate configuration parameters.
        
        Raises:
            ValueError: If configuration is invalid
        """
        # Validate report types
        valid_types = [t.value for t in ReportType]
        for report_type in self.report_types:
            if report_type not in valid_types:
                raise ValueError(f"Invalid report type: {report_type}. Valid types: {valid_types}")
        
        # Validate output format
        valid_formats = [f.value for f in OutputFormat]
        if self.output_format not in valid_formats:
            raise ValueError(f"Invalid output format: {self.output_format}. Valid formats: {valid_formats}")
        
        # Validate status
        valid_statuses = [s.value for s in ReportStatus]
        if self.status not in valid_statuses:
            raise ValueError(f"Invalid status: {self.status}. Valid statuses: {valid_statuses}")
        
        # Validate improvement notation if provided
        if self.improvement_notation:
            valid_notations = [n.value for n in ImprovementNotation]
            if self.improvement_notation not in valid_notations:
                raise ValueError(f"Invalid improvement notation: {self.improvement_notation}. Valid notations: {valid_notations}")
        
        # Validate reporting period if provided
        if self.reporting_period:
            required_keys = ['start', 'end']
            for key in required_keys:
                if key not in self.reporting_period:
                    raise ValueError(f"Reporting period must contain '{key}' key")
        
        # Bundle output requires at least one report type
        if self.output_format == OutputFormat.BUNDLE.value and not self.report_types:
            raise ValueError("Bundle output requires at least one report type")
        
        logger.debug(f"MeasureReportConfig validated successfully: {len(self.report_types)} report types")
    
    @classmethod
    def individual_only(cls, measure_reference: Optional[str] = None, **kwargs) -> 'MeasureReportConfig':
        """
        Create configuration for individual patient reports only.
        
        Args:
            measure_reference: Reference to the Measure resource
            **kwargs: Additional configuration options
            
        Returns:
            Configured instance for individual reports
        """
        return cls(
            report_types=[ReportType.INDIVIDUAL.value],
            measure_reference=measure_reference,
            **kwargs
        )
    
    @classmethod
    def population_only(cls, measure_reference: Optional[str] = None, **kwargs) -> 'MeasureReportConfig':
        """
        Create configuration for population-level (subject-list) reports only.
        
        Args:
            measure_reference: Reference to the Measure resource
            **kwargs: Additional configuration options
            
        Returns:
            Configured instance for population reports
        """
        return cls(
            report_types=[ReportType.SUBJECT_LIST.value],
            measure_reference=measure_reference,
            **kwargs
        )
    
    @classmethod
    def both_reports(cls, measure_reference: Optional[str] = None, output_format: str = OutputFormat.BUNDLE.value, **kwargs) -> 'MeasureReportConfig':
        """
        Create configuration for both individual and population reports.
        
        Args:
            measure_reference: Reference to the Measure resource
            output_format: Output format (defaults to bundle for multiple reports)
            **kwargs: Additional configuration options
            
        Returns:
            Configured instance for both report types
        """
        return cls(
            report_types=[ReportType.INDIVIDUAL.value, ReportType.SUBJECT_LIST.value],
            output_format=output_format,
            measure_reference=measure_reference,
            **kwargs
        )
    
    def with_reporting_period(self, start_date: str, end_date: str) -> 'MeasureReportConfig':
        """
        Add reporting period to configuration.
        
        Args:
            start_date: Start date in ISO format (YYYY-MM-DD or full ISO datetime)
            end_date: End date in ISO format (YYYY-MM-DD or full ISO datetime)
            
        Returns:
            Self for method chaining
        """
        self.reporting_period = {
            'start': start_date,
            'end': end_date
        }
        self.validate()
        return self
    
    def with_population_mapping(self, mapping: Dict[str, str]) -> 'MeasureReportConfig':
        """
        Add custom population mapping to configuration.
        
        Args:
            mapping: Dictionary mapping define names to FHIR population types
                    (e.g., {"Initial Population": "initial-population"})
        
        Returns:
            Self for method chaining
        """
        self.population_mapping = mapping.copy()
        return self
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize configuration to dictionary.
        
        Returns:
            Dictionary representation of configuration
        """
        return {
            'report_types': self.report_types,
            'output_format': self.output_format,
            'include_evaluated_resources': self.include_evaluated_resources,
            'measure_reference': self.measure_reference,
            'reporting_period': self.reporting_period,
            'status': self.status,
            'improvement_notation': self.improvement_notation,
            'reporter': self.reporter,
            'population_mapping': self.population_mapping,
            'bundle_type': self.bundle_type,
            'auto_generate_ids': self.auto_generate_ids,
            'include_subject_details': self.include_subject_details
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MeasureReportConfig':
        """
        Deserialize configuration from dictionary.
        
        Args:
            data: Dictionary representation of configuration
            
        Returns:
            MeasureReportConfig instance
        """
        return cls(**data)
    
    def to_json(self) -> str:
        """
        Serialize configuration to JSON string.
        
        Returns:
            JSON string representation
        """
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'MeasureReportConfig':
        """
        Deserialize configuration from JSON string.
        
        Args:
            json_str: JSON string representation
            
        Returns:
            MeasureReportConfig instance
        """
        data = json.loads(json_str)
        return cls.from_dict(data)
    
    def is_individual_report_enabled(self) -> bool:
        """Check if individual reports are enabled."""
        return ReportType.INDIVIDUAL.value in self.report_types
    
    def is_subject_list_report_enabled(self) -> bool:
        """Check if subject-list reports are enabled."""
        return ReportType.SUBJECT_LIST.value in self.report_types
    
    def requires_bundle_output(self) -> bool:
        """Check if bundle output is required."""
        return (self.output_format == OutputFormat.BUNDLE.value or 
                len(self.report_types) > 1)
    
    def get_population_type(self, define_name: str) -> Optional[str]:
        """
        Get FHIR population type for a CQL define name.
        
        Args:
            define_name: CQL define statement name
            
        Returns:
            FHIR population type if mapped, None otherwise
        """
        if self.population_mapping:
            return self.population_mapping.get(define_name)
        
        # Default mapping based on common patterns
        define_lower = define_name.lower()
        
        if 'initial population' in define_lower:
            return 'initial-population'
        elif 'denominator exclusion' in define_lower:
            return 'denominator-exclusion'
        elif 'denominator exception' in define_lower:
            return 'denominator-exception'
        elif 'denominator' in define_lower:
            return 'denominator'
        elif 'numerator exclusion' in define_lower:
            return 'numerator-exclusion'
        elif 'numerator' in define_lower:
            return 'numerator'
        elif 'measure population exclusion' in define_lower:
            return 'measure-population-exclusion'
        elif 'measure population' in define_lower:
            return 'measure-population'
        elif 'measure observation' in define_lower:
            return 'measure-observation'
        
        return None
    
    def __str__(self) -> str:
        """String representation of configuration."""
        return (f"MeasureReportConfig(types={self.report_types}, "
                f"format={self.output_format}, "
                f"measure={self.measure_reference})")
    
    def __repr__(self) -> str:
        """Developer representation of configuration."""
        return f"MeasureReportConfig({self.to_dict()})"

# Default configurations for common use cases
DEFAULT_INDIVIDUAL_CONFIG = MeasureReportConfig.individual_only()
DEFAULT_POPULATION_CONFIG = MeasureReportConfig.population_only()
DEFAULT_BOTH_CONFIG = MeasureReportConfig.both_reports()