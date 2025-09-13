"""
MeasureReport Generator - Transforms CQL results to FHIR MeasureReport resources.

This module provides generators for creating FHIR-compliant MeasureReport resources
from CQL execution results, supporting individual and population-level reporting.
"""

import logging
import uuid
from typing import Dict, Any, List, Union, Optional, TYPE_CHECKING
from datetime import datetime
from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from .measure_report_config import MeasureReportConfig

logger = logging.getLogger(__name__)

class BaseMeasureReportGenerator(ABC):
    """Base class for MeasureReport generators."""
    
    def __init__(self, config: 'MeasureReportConfig'):
        """Initialize generator with configuration."""
        self.config = config
        logger.debug(f"MeasureReportGenerator initialized with config: {config}")
    
    @abstractmethod
    def generate(self, execution_summary: Dict[str, Any]) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Generate MeasureReport resource(s)."""
        pass
    
    def _extract_patient_ids(self, execution_summary: Dict[str, Any]) -> List[str]:
        """Extract unique patient IDs from execution results."""
        patient_ids = set()
        
        # First, try to get patient IDs from patient-level results
        define_results = execution_summary.get("define_results", {})
        for define_name, results in define_results.items():
            if isinstance(results, list):
                for result in results:
                    if isinstance(result, dict) and "patient_id" in result:
                        patient_ids.add(result["patient_id"])
        
        # If no patient IDs found, try to infer from library-level results
        # This is a fallback for library-level evaluation
        if not patient_ids:
            logger.info("No patient-level results found, attempting to extract patients from library-level results")
            
            # Look for Patient resources or patient references in the results
            for define_name, results in define_results.items():
                if isinstance(results, list):
                    for result in results:
                        # Try to extract patient ID from various result formats
                        patient_id = self._extract_patient_id_from_result(result)
                        if patient_id:
                            patient_ids.add(patient_id)
        
        # Final fallback: if still no patient IDs found, check if we have any results at all
        if not patient_ids:
            # If define_results is empty, return empty list (no reports should be generated)
            if not define_results:
                logger.info("No define results found - returning empty patient list for empty bundle")
                return []
            
            # If we have results but no patient IDs, use generic placeholder
            # This ensures MeasureReport generation can proceed even with library-level results  
            logger.warning("No patient IDs could be extracted from execution results.")
            logger.info("This may indicate library-level evaluation rather than patient-level evaluation.")
            logger.info("Consider implementing patient-level CQL evaluation for accurate MeasureReports.")
            # Use generic placeholder - consuming system should handle appropriately
            patient_ids = {"patient-unknown"}
            
        return sorted(list(patient_ids))
    
    def _extract_patient_id_from_result(self, result: Any) -> Optional[str]:
        """Try to extract a patient ID from a result object."""
        if isinstance(result, dict):
            # Check for direct patient_id field
            if "patient_id" in result:
                return result["patient_id"]
                
            # Check for Patient resource
            if result.get("resourceType") == "Patient":
                return result.get("id")
                
            # Check for subject reference
            if "subject" in result and isinstance(result["subject"], dict):
                reference = result["subject"].get("reference", "")
                if reference.startswith("Patient/"):
                    return reference.split("/", 1)[1]
        
        # Check if result is a Patient resource JSON string
        if isinstance(result, str):
            try:
                import json
                parsed = json.loads(result)
                if isinstance(parsed, dict) and parsed.get("resourceType") == "Patient":
                    return parsed.get("id")
            except:
                pass
                
        return None
    
    def _get_population_results_for_patient(self, execution_summary: Dict[str, Any], 
                                          patient_id: str) -> Dict[str, bool]:
        """Get population membership results for a specific patient."""
        population_results = {}
        
        define_results = execution_summary.get("define_results", {})
        for define_name, results in define_results.items():
            # Map CQL define name to FHIR population type
            population_type = self.config.get_population_type(define_name)
            if population_type:
                # Find result for this patient
                patient_result = self._find_patient_result(results, patient_id)
                if patient_result is not None:
                    # Convert result to boolean (1 means patient is in population)
                    is_in_population = self._convert_to_boolean(patient_result)
                    population_results[population_type] = is_in_population
                    logger.debug(f"Patient {patient_id} in {population_type}: {is_in_population}")
        
        return population_results
    
    def _find_patient_result(self, results: Any, patient_id: str) -> Any:
        """Find result for specific patient from CQL execution results."""
        if isinstance(results, list):
            for result in results:
                if isinstance(result, dict) and result.get("patient_id") == patient_id:
                    # Check various result formats
                    if "result" in result:
                        if isinstance(result["result"], dict) and "expression_result" in result["result"]:
                            return result["result"]["expression_result"]
                        else:
                            return result["result"]
                    else:
                        # Fallback to looking for boolean-like fields
                        for key in ["value", "expression_result", "outcome"]:
                            if key in result:
                                return result[key]
        return None
    
    def _convert_to_boolean(self, result: Any) -> bool:
        """Convert CQL result to boolean population membership."""
        if isinstance(result, bool):
            return result
        elif isinstance(result, (int, float)):
            return result != 0
        elif isinstance(result, str):
            # Handle string representations
            if result.lower() in ["true", "1", "yes", "t"]:
                return True
            elif result.lower() in ["false", "0", "no", "f"]:
                return False
            else:
                # Non-empty string is truthy
                return len(result.strip()) > 0
        elif result is None:
            return False
        else:
            # Other types - non-None is truthy
            return result is not None
    
    def _aggregate_population_counts(self, execution_summary: Dict[str, Any]) -> Dict[str, int]:
        """Aggregate population counts across all patients."""
        population_counts = {}
        patient_ids = self._extract_patient_ids(execution_summary)
        
        # Initialize counts
        define_results = execution_summary.get("define_results", {})
        for define_name in define_results.keys():
            population_type = self.config.get_population_type(define_name)
            if population_type:
                population_counts[population_type] = 0
        
        # Count patients in each population
        for patient_id in patient_ids:
            patient_populations = self._get_population_results_for_patient(execution_summary, patient_id)
            for population_type, is_in_population in patient_populations.items():
                if is_in_population:
                    population_counts[population_type] += 1
        
        return population_counts
    
    def _create_subject_lists(self, execution_summary: Dict[str, Any]) -> Dict[str, List[str]]:
        """Create subject lists for each population."""
        subject_lists = {}
        patient_ids = self._extract_patient_ids(execution_summary)
        
        # Initialize lists
        define_results = execution_summary.get("define_results", {})
        for define_name in define_results.keys():
            population_type = self.config.get_population_type(define_name)
            if population_type:
                subject_lists[population_type] = []
        
        # Add patients to appropriate lists
        for patient_id in patient_ids:
            patient_populations = self._get_population_results_for_patient(execution_summary, patient_id)
            for population_type, is_in_population in patient_populations.items():
                if is_in_population:
                    subject_lists[population_type].append(patient_id)
        
        return subject_lists

class IndividualMeasureReportGenerator(BaseMeasureReportGenerator):
    """Generator for individual patient MeasureReports."""
    
    def generate(self, execution_summary: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Generate individual MeasureReport for each patient.
        
        Args:
            execution_summary: CQL execution results
            
        Returns:
            List of individual MeasureReport resources
        """
        from .fhir_measure_report import IndividualMeasureReport, FHIRPeriod
        
        logger.info("Generating individual MeasureReports")
        
        patient_ids = self._extract_patient_ids(execution_summary)
        reports = []
        
        for patient_id in patient_ids:
            logger.debug(f"Generating individual report for patient {patient_id}")
            
            # Create individual report
            report = IndividualMeasureReport(
                patient_reference=f"Patient/{patient_id}",
                measure=self.config.measure_reference or f"Measure/{execution_summary.get('library_id', 'unknown')}",
                status=self.config.status,
                date=execution_summary.get("execution_timestamp", datetime.now().isoformat()),
                period=self._create_period() if self.config.reporting_period else None,
                improvementNotation=self._create_improvement_notation() if self.config.improvement_notation else None
            )
            
            # Get population results for this patient
            patient_populations = self._get_population_results_for_patient(execution_summary, patient_id)
            
            # Add population results
            for population_type, is_in_population in patient_populations.items():
                count = 1 if is_in_population else 0
                report.add_population_result(population_type, count)
                logger.debug(f"Added {population_type} = {count} for patient {patient_id}")
            
            # Ensure we have at least one group (FHIR requirement)
            if not report.group:
                from .fhir_measure_report import MeasureReportGroup
                report.group.append(MeasureReportGroup())
            
            # Add evaluated resources if configured
            if self.config.include_evaluated_resources:
                self._add_evaluated_resources(report, patient_id, execution_summary)
            
            reports.append(report.to_dict())
        
        logger.info(f"Generated {len(reports)} individual MeasureReports")
        return reports
    
    def _create_period(self) -> 'FHIRPeriod':
        """Create FHIR Period from configuration."""
        from .fhir_measure_report import FHIRPeriod
        
        if self.config.reporting_period:
            return FHIRPeriod(
                start=self.config.reporting_period.get("start"),
                end=self.config.reporting_period.get("end")
            )
        return None
    
    def _create_improvement_notation(self) -> Dict[str, Any]:
        """Create improvement notation CodeableConcept."""
        if not self.config.improvement_notation:
            return None
        
        return {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/measure-improvement-notation",
                    "code": self.config.improvement_notation,
                    "display": self._get_improvement_notation_display(self.config.improvement_notation)
                }
            ]
        }
    
    def _get_improvement_notation_display(self, notation: str) -> str:
        """Get display text for improvement notation."""
        display_map = {
            "increase": "Increased score indicates improvement",
            "decrease": "Decreased score indicates improvement"
        }
        return display_map.get(notation, notation.title())
    
    def _add_evaluated_resources(self, report: 'IndividualMeasureReport', 
                               patient_id: str, execution_summary: Dict[str, Any]) -> None:
        """Add evaluated resource references for this patient."""
        # Add patient reference
        report.add_evaluated_resource(f"Patient/{patient_id}")
        
        # Could add other resource references based on what was evaluated
        # This would need to be enhanced based on actual CQL evaluation context

class SubjectListMeasureReportGenerator(BaseMeasureReportGenerator):
    """Generator for subject-list (population) MeasureReports."""
    
    def generate(self, execution_summary: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate subject-list MeasureReport for the population.
        
        Args:
            execution_summary: CQL execution results
            
        Returns:
            Subject-list MeasureReport resource
        """
        from .fhir_measure_report import SubjectListMeasureReport, FHIRPeriod
        
        logger.info("Generating subject-list MeasureReport")
        
        # Create subject-list report
        report = SubjectListMeasureReport(
            measure=self.config.measure_reference or f"Measure/{execution_summary.get('library_id', 'unknown')}",
            status=self.config.status,
            date=execution_summary.get("execution_timestamp", datetime.now().isoformat()),
            period=self._create_period() if self.config.reporting_period else None,
            improvementNotation=self._create_improvement_notation() if self.config.improvement_notation else None
        )
        
        # Get population counts and subject lists
        population_counts = self._aggregate_population_counts(execution_summary)
        subject_lists = self._create_subject_lists(execution_summary) if self.config.include_subject_details else None
        
        # Add population results
        for population_type, count in population_counts.items():
            subjects = subject_lists.get(population_type) if subject_lists else None
            report.add_population_result(population_type, count, subjects)
            logger.debug(f"Added {population_type} = {count} subjects")
        
        # Calculate and add measure scores if applicable
        self._add_measure_scores(report, population_counts)
        
        # Ensure we have at least one group (FHIR requirement)
        if not report.group:
            from .fhir_measure_report import MeasureReportGroup
            report.group.append(MeasureReportGroup())
        
        # Add evaluated resources if configured
        if self.config.include_evaluated_resources:
            self._add_evaluated_resources(report, execution_summary)
        
        logger.info("Generated subject-list MeasureReport")
        return report.to_dict()
    
    def _create_period(self) -> 'FHIRPeriod':
        """Create FHIR Period from configuration."""
        from .fhir_measure_report import FHIRPeriod
        
        if self.config.reporting_period:
            return FHIRPeriod(
                start=self.config.reporting_period.get("start"),
                end=self.config.reporting_period.get("end")
            )
        return None
    
    def _create_improvement_notation(self) -> Dict[str, Any]:
        """Create improvement notation CodeableConcept."""
        if not self.config.improvement_notation:
            return None
        
        return {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/measure-improvement-notation",
                    "code": self.config.improvement_notation,
                    "display": self._get_improvement_notation_display(self.config.improvement_notation)
                }
            ]
        }
    
    def _get_improvement_notation_display(self, notation: str) -> str:
        """Get display text for improvement notation."""
        display_map = {
            "increase": "Increased score indicates improvement",
            "decrease": "Decreased score indicates improvement"
        }
        return display_map.get(notation, notation.title())
    
    def _add_measure_scores(self, report: 'SubjectListMeasureReport', population_counts: Dict[str, int]) -> None:
        """Add measure scores based on population counts."""
        # Calculate proportion scores (numerator/denominator)
        if "numerator" in population_counts and "denominator" in population_counts:
            numerator = population_counts["numerator"]
            denominator = population_counts["denominator"]
            
            if denominator > 0:
                score = numerator / denominator
                report.set_measure_score(0, score, "1")  # "1" is the unit for proportions
                logger.debug(f"Added measure score: {numerator}/{denominator} = {score:.4f}")
    
    def _add_evaluated_resources(self, report: 'SubjectListMeasureReport', 
                               execution_summary: Dict[str, Any]) -> None:
        """Add evaluated resource references."""
        # Add patient references for all evaluated patients
        patient_ids = self._extract_patient_ids(execution_summary)
        for patient_id in patient_ids:
            report.add_evaluated_resource(f"Patient/{patient_id}")

class MeasureReportGenerator:
    """
    Main generator for creating FHIR MeasureReport resources from CQL execution results.
    
    Orchestrates the creation of individual and/or subject-list reports based on configuration.
    """
    
    def __init__(self, config: 'MeasureReportConfig'):
        """
        Initialize the generator.
        
        Args:
            config: MeasureReport configuration
        """
        self.config = config
        logger.debug(f"MeasureReportGenerator initialized with config: {config}")
    
    def generate_reports(self, execution_summary: Dict[str, Any]) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Generate MeasureReport resources from CQL execution summary.
        
        Args:
            execution_summary: CQL execution results
            
        Returns:
            MeasureReport resource(s) or Bundle containing MeasureReports
        """
        logger.info(f"Generating MeasureReports: types={self.config.report_types}, format={self.config.output_format}")
        
        reports = []
        
        # Generate individual reports if requested
        if self.config.is_individual_report_enabled():
            individual_generator = IndividualMeasureReportGenerator(self.config)
            individual_reports = individual_generator.generate(execution_summary)
            reports.extend(individual_reports)
        
        # Generate subject-list report if requested
        if self.config.is_subject_list_report_enabled():
            subject_list_generator = SubjectListMeasureReportGenerator(self.config)
            subject_list_report = subject_list_generator.generate(execution_summary)
            reports.append(subject_list_report)
        
        # Return appropriate format based on configuration
        if self.config.requires_bundle_output():
            return self._create_bundle(reports)
        elif len(reports) == 1:
            return reports[0]
        else:
            return reports
    
    def _create_bundle(self, reports: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create Bundle resource containing MeasureReports."""
        from .fhir_measure_report import Bundle
        
        bundle = Bundle(type=self.config.bundle_type)
        
        for report in reports:
            bundle.add_resource(report)
        
        logger.info(f"Created Bundle with {len(reports)} MeasureReports")
        return bundle.to_dict()