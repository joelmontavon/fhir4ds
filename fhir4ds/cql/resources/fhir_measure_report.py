"""
FHIR MeasureReport Resource Classes

This module provides FHIR R4 compliant MeasureReport resource classes
for representing quality measure evaluation results.
"""

import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

@dataclass
class FHIRPeriod:
    """FHIR Period data type."""
    start: Optional[str] = None
    end: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {}
        if self.start:
            result["start"] = self.start
        if self.end:
            result["end"] = self.end
        return result

@dataclass
class FHIRReference:
    """FHIR Reference data type."""
    reference: Optional[str] = None
    type: Optional[str] = None
    identifier: Optional[Dict[str, Any]] = None
    display: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {}
        if self.reference:
            result["reference"] = self.reference
        if self.type:
            result["type"] = self.type
        if self.identifier:
            result["identifier"] = self.identifier
        if self.display:
            result["display"] = self.display
        return result

@dataclass
class MeasureReportPopulation:
    """Population component of a MeasureReport group."""
    code: Optional[Dict[str, Any]] = None  # CodeableConcept
    count: Optional[int] = None
    subjectResults: Optional[FHIRReference] = None  # Reference to List resource
    subjects: Optional[FHIRReference] = None  # Reference to List resource (FHIR R5)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {}
        if self.code:
            result["code"] = self.code
        if self.count is not None:
            result["count"] = self.count
        if self.subjectResults:
            result["subjectResults"] = self.subjectResults.to_dict()
        if self.subjects:
            result["subjects"] = self.subjects.to_dict()
        return result

@dataclass
class MeasureReportStratifier:
    """Stratifier component of a MeasureReport group."""
    code: Optional[List[Dict[str, Any]]] = None  # List of CodeableConcept
    stratum: List[Dict[str, Any]] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {}
        if self.code:
            result["code"] = self.code
        if self.stratum:
            result["stratum"] = self.stratum
        return result

@dataclass
class MeasureReportGroup:
    """Group component of a MeasureReport."""
    id: Optional[str] = None
    code: Optional[Dict[str, Any]] = None  # CodeableConcept
    population: List[MeasureReportPopulation] = field(default_factory=list)
    measureScore: Optional[Dict[str, Any]] = None  # Quantity
    stratifier: List[MeasureReportStratifier] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {}
        if self.id:
            result["id"] = self.id
        if self.code:
            result["code"] = self.code
        # Always include population array (FHIR requirement)
        result["population"] = [pop.to_dict() for pop in self.population]
        if self.measureScore:
            result["measureScore"] = self.measureScore
        if self.stratifier:
            result["stratifier"] = [strat.to_dict() for strat in self.stratifier]
        return result

class FHIRMeasureReport(ABC):
    """
    Base class for FHIR R4 MeasureReport resources.
    
    Provides common functionality for individual and subject-list reports.
    """
    
    def __init__(self,
                 id: Optional[str] = None,
                 status: str = "complete",
                 type: str = "subject-list",
                 measure: str = "",
                 subject: Optional[FHIRReference] = None,
                 date: Optional[str] = None,
                 reporter: Optional[FHIRReference] = None,
                 period: Optional[FHIRPeriod] = None,
                 improvementNotation: Optional[Dict[str, Any]] = None,
                 group: Optional[List[MeasureReportGroup]] = None,
                 evaluatedResource: Optional[List[FHIRReference]] = None):
        """
        Initialize FHIR MeasureReport.
        
        Args:
            id: Resource identifier
            status: Report status (complete, pending, error)
            type: Report type (individual, subject-list, summary, data-collection)
            measure: Reference to Measure resource
            subject: Subject of the report (for individual reports)
            date: Date report was generated
            reporter: Organization that generated the report
            period: Reporting period
            improvementNotation: Improvement notation
            group: Population groups
            evaluatedResource: Resources evaluated during calculation
        """
        self.resourceType = "MeasureReport"
        self.id = id or self._generate_id()
        self.status = status
        self.type = type
        self.measure = measure
        self.subject = subject
        self.date = date or datetime.now().isoformat()
        self.reporter = reporter
        self.period = period
        self.improvementNotation = improvementNotation
        self.group = group or []
        self.evaluatedResource = evaluatedResource or []
    
    def _generate_id(self) -> str:
        """Generate unique resource ID."""
        return str(uuid.uuid4())
    
    @abstractmethod
    def add_population_result(self, population_type: str, count: int, subjects: Optional[List[str]] = None) -> None:
        """
        Add population result to the report.
        
        Args:
            population_type: Type of population (e.g., 'initial-population', 'numerator')
            count: Number of subjects in population
            subjects: List of subject IDs (for subject-list reports)
        """
        pass
    
    def add_evaluated_resource(self, resource_reference: str) -> None:
        """
        Add evaluated resource reference.
        
        Args:
            resource_reference: Reference to evaluated resource
        """
        self.evaluatedResource.append(FHIRReference(reference=resource_reference))
    
    def set_measure_score(self, group_index: int, score_value: float, score_unit: str = "1") -> None:
        """
        Set measure score for a group.
        
        Args:
            group_index: Index of the group to update
            score_value: Score value
            score_unit: Score unit (default: "1" for ratios)
        """
        if group_index < len(self.group):
            self.group[group_index].measureScore = {
                "value": score_value,
                "unit": score_unit,
                "system": "http://unitsofmeasure.org",
                "code": score_unit
            }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation suitable for JSON serialization."""
        result = {
            "resourceType": self.resourceType,
            "id": self.id,
            "status": self.status,
            "type": self.type,
            "measure": self.measure,
            "date": self.date
        }
        
        if self.subject:
            result["subject"] = self.subject.to_dict()
        
        if self.reporter:
            result["reporter"] = self.reporter.to_dict()
        
        if self.period:
            if isinstance(self.period, FHIRPeriod):
                result["period"] = self.period.to_dict()
            else:
                result["period"] = self.period
        
        if self.improvementNotation:
            result["improvementNotation"] = self.improvementNotation
        
        if self.group:
            result["group"] = [group.to_dict() for group in self.group]
        
        if self.evaluatedResource:
            result["evaluatedResource"] = [ref.to_dict() for ref in self.evaluatedResource]
        
        return result
    
    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

class IndividualMeasureReport(FHIRMeasureReport):
    """
    Individual MeasureReport for a single patient.
    
    Reports measure evaluation results for a specific patient.
    """
    
    def __init__(self,
                 patient_reference: str,
                 measure: str,
                 **kwargs):
        """
        Initialize individual MeasureReport.
        
        Args:
            patient_reference: Reference to the patient (e.g., "Patient/123")
            measure: Reference to the measure resource
            **kwargs: Additional MeasureReport parameters
        """
        super().__init__(
            type="individual",
            measure=measure,
            subject=FHIRReference(reference=patient_reference),
            **kwargs
        )
    
    def add_population_result(self, population_type: str, count: int, subjects: Optional[List[str]] = None) -> None:
        """
        Add population result for individual report.
        
        Args:
            population_type: Type of population
            count: Count (0 or 1 for individual reports)
            subjects: Not used for individual reports
        """
        # Ensure we have at least one group
        if not self.group:
            self.group.append(MeasureReportGroup())
        
        # Create population with appropriate coding
        population = MeasureReportPopulation(
            code=self._create_population_code(population_type),
            count=count
        )
        
        self.group[0].population.append(population)
    
    def _create_population_code(self, population_type: str) -> Dict[str, Any]:
        """Create CodeableConcept for population type."""
        return {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/measure-population",
                    "code": population_type,
                    "display": self._get_population_display(population_type)
                }
            ]
        }
    
    def _get_population_display(self, population_type: str) -> str:
        """Get display text for population type."""
        display_map = {
            "initial-population": "Initial Population",
            "numerator": "Numerator",
            "numerator-exclusion": "Numerator Exclusion",
            "denominator": "Denominator",
            "denominator-exclusion": "Denominator Exclusion",
            "denominator-exception": "Denominator Exception",
            "measure-population": "Measure Population",
            "measure-population-exclusion": "Measure Population Exclusion",
            "measure-observation": "Measure Observation"
        }
        return display_map.get(population_type, population_type.title())

class SubjectListMeasureReport(FHIRMeasureReport):
    """
    Subject-list MeasureReport for population-level results.
    
    Reports aggregate measure evaluation results for a population.
    """
    
    def __init__(self,
                 measure: str,
                 **kwargs):
        """
        Initialize subject-list MeasureReport.
        
        Args:
            measure: Reference to the measure resource
            **kwargs: Additional MeasureReport parameters
        """
        super().__init__(
            type="subject-list",
            measure=measure,
            **kwargs
        )
    
    def add_population_result(self, population_type: str, count: int, subjects: Optional[List[str]] = None) -> None:
        """
        Add population result for subject-list report.
        
        Args:
            population_type: Type of population
            count: Number of subjects in population
            subjects: List of subject IDs in population
        """
        # Ensure we have at least one group
        if not self.group:
            self.group.append(MeasureReportGroup())
        
        # Create population with appropriate coding
        population = MeasureReportPopulation(
            code=self._create_population_code(population_type),
            count=count
        )
        
        # Add subject list reference if subjects provided
        if subjects and len(subjects) > 0:
            # In a real implementation, you'd create a List resource and reference it
            # For now, we'll store the subject count
            population.subjectResults = FHIRReference(
                reference=f"List/subjects-{population_type}-{self.id}",
                display=f"{len(subjects)} subjects"
            )
        
        self.group[0].population.append(population)
    
    def add_stratification(self, 
                          stratifier_code: str,
                          stratum_value: str,
                          population_results: Dict[str, int]) -> None:
        """
        Add stratification results.
        
        Args:
            stratifier_code: Code identifying the stratifier (e.g., 'gender', 'age-group')
            stratum_value: Value for this stratum (e.g., 'male', '18-65')
            population_results: Population counts for this stratum
        """
        # Ensure we have at least one group
        if not self.group:
            self.group.append(MeasureReportGroup())
        
        # Find or create stratifier
        stratifier = None
        for existing_stratifier in self.group[0].stratifier:
            if (existing_stratifier.code and 
                len(existing_stratifier.code) > 0 and
                existing_stratifier.code[0].get("coding", [{}])[0].get("code") == stratifier_code):
                stratifier = existing_stratifier
                break
        
        if not stratifier:
            stratifier = MeasureReportStratifier(
                code=[{
                    "coding": [
                        {
                            "system": "http://example.org/stratifiers",
                            "code": stratifier_code,
                            "display": stratifier_code.replace('-', ' ').title()
                        }
                    ]
                }]
            )
            self.group[0].stratifier.append(stratifier)
        
        # Create stratum
        stratum_populations = []
        for pop_type, count in population_results.items():
            stratum_populations.append({
                "code": self._create_population_code(pop_type),
                "count": count
            })
        
        stratum = {
            "value": {
                "coding": [
                    {
                        "system": "http://example.org/stratifier-values",
                        "code": stratum_value,
                        "display": stratum_value.replace('-', ' ').title()
                    }
                ]
            },
            "population": stratum_populations
        }
        
        stratifier.stratum.append(stratum)
    
    def _create_population_code(self, population_type: str) -> Dict[str, Any]:
        """Create CodeableConcept for population type."""
        return {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/measure-population",
                    "code": population_type,
                    "display": self._get_population_display(population_type)
                }
            ]
        }
    
    def _get_population_display(self, population_type: str) -> str:
        """Get display text for population type."""
        display_map = {
            "initial-population": "Initial Population",
            "numerator": "Numerator",
            "numerator-exclusion": "Numerator Exclusion",
            "denominator": "Denominator",
            "denominator-exclusion": "Denominator Exclusion",
            "denominator-exception": "Denominator Exception",
            "measure-population": "Measure Population",
            "measure-population-exclusion": "Measure Population Exclusion",
            "measure-observation": "Measure Observation"
        }
        return display_map.get(population_type, population_type.title())

@dataclass
class Bundle:
    """
    FHIR Bundle resource for containing multiple MeasureReports.
    """
    resourceType: str = "Bundle"
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    type: str = "collection"  # collection, document, message, transaction, etc.
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    total: int = 0
    entry: List[Dict[str, Any]] = field(default_factory=list)
    
    def add_resource(self, resource: Union[FHIRMeasureReport, Dict[str, Any]]) -> None:
        """
        Add a resource to the bundle.
        
        Args:
            resource: MeasureReport resource or dictionary representation
        """
        if isinstance(resource, FHIRMeasureReport):
            resource_dict = resource.to_dict()
        else:
            resource_dict = resource
        
        entry = {
            "fullUrl": f"MeasureReport/{resource_dict.get('id', str(uuid.uuid4()))}",
            "resource": resource_dict
        }
        
        self.entry.append(entry)
        self.total = len(self.entry)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "resourceType": self.resourceType,
            "id": self.id,
            "type": self.type,
            "timestamp": self.timestamp,
            "total": self.total,
            "entry": self.entry
        }
    
    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

def create_individual_report(patient_id: str, 
                           measure_reference: str,
                           population_results: Dict[str, int],
                           **kwargs) -> IndividualMeasureReport:
    """
    Convenience function to create individual MeasureReport.
    
    Args:
        patient_id: Patient identifier
        measure_reference: Reference to measure resource
        population_results: Dictionary of population type -> count
        **kwargs: Additional parameters
        
    Returns:
        IndividualMeasureReport instance
    """
    report = IndividualMeasureReport(
        patient_reference=f"Patient/{patient_id}",
        measure=measure_reference,
        **kwargs
    )
    
    for pop_type, count in population_results.items():
        report.add_population_result(pop_type, count)
    
    return report

def create_subject_list_report(measure_reference: str,
                             population_results: Dict[str, int],
                             subject_lists: Optional[Dict[str, List[str]]] = None,
                             **kwargs) -> SubjectListMeasureReport:
    """
    Convenience function to create subject-list MeasureReport.
    
    Args:
        measure_reference: Reference to measure resource
        population_results: Dictionary of population type -> count
        subject_lists: Dictionary of population type -> list of subject IDs
        **kwargs: Additional parameters
        
    Returns:
        SubjectListMeasureReport instance
    """
    report = SubjectListMeasureReport(
        measure=measure_reference,
        **kwargs
    )
    
    for pop_type, count in population_results.items():
        subjects = subject_lists.get(pop_type) if subject_lists else None
        report.add_population_result(pop_type, count, subjects)
    
    return report

def create_bundle_with_reports(reports: List[FHIRMeasureReport], 
                              bundle_type: str = "collection") -> Bundle:
    """
    Convenience function to create Bundle with MeasureReports.
    
    Args:
        reports: List of MeasureReport resources
        bundle_type: Bundle type
        
    Returns:
        Bundle instance
    """
    bundle = Bundle(type=bundle_type)
    
    for report in reports:
        bundle.add_resource(report)
    
    return bundle