"""
Code Appropriateness Scoring Engine

Provides scoring mechanisms for clinical code appropriateness in healthcare contexts.
Evaluates codes based on specificity, currency, context relevance, and other clinical factors.

Phase 4: Navigation Enhancement - Code Appropriateness Scoring
"""

import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
from datetime import datetime, date

from .code_validator import CodeValidator, ValidationResult, CodeValidationResult

logger = logging.getLogger(__name__)


class ClinicalContext(Enum):
    """Clinical context categories for appropriateness scoring."""
    DIAGNOSIS = "diagnosis"
    PROCEDURE = "procedure"
    MEDICATION = "medication"
    LABORATORY = "laboratory"
    OBSERVATION = "observation"
    ENCOUNTER = "encounter"
    GENERAL = "general"


class SpecificityLevel(Enum):
    """Code specificity levels for granularity assessment."""
    VERY_SPECIFIC = "very_specific"  # Highly granular codes
    SPECIFIC = "specific"           # Appropriate specificity
    MODERATE = "moderate"           # Some specificity
    GENERAL = "general"             # Generic codes
    UNSPECIFIED = "unspecified"     # Unspecified/NOS codes


@dataclass
class AppropriatenessScore:
    """Comprehensive appropriateness score for a clinical code."""
    code: str
    system: str
    overall_score: float  # 0.0 to 1.0
    
    # Component scores (0.0 to 1.0)
    specificity_score: float
    currency_score: float
    context_score: float
    validity_score: float
    
    # Detailed metrics
    specificity_level: SpecificityLevel
    context_relevance: ClinicalContext
    issues: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    
    # Metadata
    scoring_method: str = "default"
    computed_at: datetime = field(default_factory=datetime.now)


class CodeAppropriatenessScorer(ABC):
    """Abstract base class for code appropriateness scoring implementations."""
    
    def __init__(self, code_validator: Optional[CodeValidator] = None):
        self.code_validator = code_validator
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    @abstractmethod
    def score_code(self, code: str, system: str, clinical_context: ClinicalContext = ClinicalContext.GENERAL,
                   additional_context: Optional[Dict[str, Any]] = None) -> AppropriatenessScore:
        """Score code appropriateness for given clinical context."""
        pass
    
    def score_codes_batch(self, codes: List[Tuple[str, str, ClinicalContext]]) -> List[AppropriatenessScore]:
        """Batch score multiple codes for efficiency."""
        scores = []
        for code, system, context in codes:
            score = self.score_code(code, system, context)
            scores.append(score)
        return scores


class DefaultAppropriatenessScorer(CodeAppropriatenessScorer):
    """Default implementation of code appropriateness scoring with configurable weights."""
    
    def __init__(self, code_validator: Optional[CodeValidator] = None, 
                 weights: Optional[Dict[str, float]] = None):
        super().__init__(code_validator)
        
        # Default scoring weights (must sum to 1.0)
        self.weights = weights or {
            'specificity': 0.35,  # Code granularity and precision
            'currency': 0.25,     # How current/modern the code is
            'context': 0.25,      # Relevance to clinical context
            'validity': 0.15      # Code validation status
        }
        
        # Validate weights sum to 1.0
        total_weight = sum(self.weights.values())
        if abs(total_weight - 1.0) > 0.01:
            raise ValueError(f"Scoring weights must sum to 1.0, got {total_weight}")
        
        # Code system patterns for scoring
        self.code_system_patterns = {
            'icd-10-cm': {
                'pattern': r'http://hl7\.org/fhir/sid/icd-10-cm',
                'context_weights': {
                    ClinicalContext.DIAGNOSIS: 1.0,
                    ClinicalContext.PROCEDURE: 0.3,
                    ClinicalContext.GENERAL: 0.8
                }
            },
            'icd-10-pcs': {
                'pattern': r'http://hl7\.org/fhir/sid/icd-10-pcs',
                'context_weights': {
                    ClinicalContext.PROCEDURE: 1.0,
                    ClinicalContext.DIAGNOSIS: 0.2,
                    ClinicalContext.GENERAL: 0.7
                }
            },
            'cpt': {
                'pattern': r'http://hl7\.org/fhir/sid/cpt',
                'context_weights': {
                    ClinicalContext.PROCEDURE: 1.0,
                    ClinicalContext.LABORATORY: 0.9,
                    ClinicalContext.GENERAL: 0.8
                }
            },
            'loinc': {
                'pattern': r'http://loinc\.org',
                'context_weights': {
                    ClinicalContext.LABORATORY: 1.0,
                    ClinicalContext.OBSERVATION: 1.0,
                    ClinicalContext.GENERAL: 0.6
                }
            },
            'snomed': {
                'pattern': r'http://snomed\.info/sct',
                'context_weights': {
                    ClinicalContext.DIAGNOSIS: 0.9,
                    ClinicalContext.PROCEDURE: 0.9,
                    ClinicalContext.OBSERVATION: 0.9,
                    ClinicalContext.GENERAL: 1.0
                }
            },
            'rxnorm': {
                'pattern': r'http://www\.nlm\.nih\.gov/research/umls/rxnorm',
                'context_weights': {
                    ClinicalContext.MEDICATION: 1.0,
                    ClinicalContext.GENERAL: 0.5
                }
            }
        }
    
    def score_code(self, code: str, system: str, clinical_context: ClinicalContext = ClinicalContext.GENERAL,
                   additional_context: Optional[Dict[str, Any]] = None) -> AppropriatenessScore:
        """Score code appropriateness using multi-dimensional analysis."""
        
        # Initialize scoring components
        specificity_score = self._score_specificity(code, system, clinical_context)
        currency_score = self._score_currency(code, system)
        context_score = self._score_context_relevance(code, system, clinical_context)
        validity_score = self._score_validity(code, system)
        
        # Calculate weighted overall score
        overall_score = (
            self.weights['specificity'] * specificity_score +
            self.weights['currency'] * currency_score +
            self.weights['context'] * context_score +
            self.weights['validity'] * validity_score
        )
        
        # Determine specificity level
        specificity_level = self._categorize_specificity(code, system, specificity_score)
        
        # Generate issues and recommendations
        issues = self._identify_issues(code, system, clinical_context, specificity_score, currency_score, context_score, validity_score)
        recommendations = self._generate_recommendations(code, system, clinical_context, issues)
        
        return AppropriatenessScore(
            code=code,
            system=system,
            overall_score=overall_score,
            specificity_score=specificity_score,
            currency_score=currency_score,
            context_score=context_score,
            validity_score=validity_score,
            specificity_level=specificity_level,
            context_relevance=clinical_context,
            issues=issues,
            recommendations=recommendations,
            scoring_method="default_weighted"
        )
    
    def _score_specificity(self, code: str, system: str, context: ClinicalContext) -> float:
        """Score code specificity/granularity (0.0 to 1.0)."""
        score = 0.5  # Base score
        
        # ICD-10-CM specificity scoring
        if 'icd-10-cm' in system.lower():
            # Length-based specificity (3-7 characters typical)
            if len(code) >= 6:
                score += 0.3  # Highly specific
            elif len(code) >= 5:
                score += 0.2  # Moderately specific
            elif len(code) >= 4:
                score += 0.1  # Some specificity
            
            # Penalize unspecified codes
            if re.search(r'[Uu]nspecified|NOS|\.9$', code):
                score -= 0.4
            
            # Reward laterality and anatomical precision
            if re.search(r'[Rr]ight|[Ll]eft|[Bb]ilateral', code):
                score += 0.1
        
        # LOINC specificity scoring
        elif 'loinc' in system.lower():
            # LOINC codes are inherently specific
            score = 0.8
            
            # Check for method/specimen specificity
            if '-' in code and len(code) >= 5:
                score += 0.1
        
        # SNOMED CT specificity scoring
        elif 'snomed' in system.lower():
            # Length-based heuristic for SNOMED CT
            if len(code) >= 8:
                score = 0.9  # SNOMED CT is highly granular
            else:
                score = 0.7
        
        # CPT specificity scoring
        elif 'cpt' in system.lower():
            score = 0.7  # CPT codes are generally specific
            
            # Category II/III codes are more specific
            if re.match(r'^[0-9]{4}[FTU]$', code):
                score += 0.2
        
        return min(1.0, max(0.0, score))
    
    def _score_currency(self, code: str, system: str) -> float:
        """Score code currency/modernity (0.0 to 1.0)."""
        score = 0.5  # Base score for unknown currency
        
        # ICD-10 is more current than ICD-9
        if 'icd-10' in system.lower():
            score = 0.9  # Modern classification
        elif 'icd-9' in system.lower():
            score = 0.3  # Legacy classification
        
        # LOINC and SNOMED CT are actively maintained
        elif 'loinc' in system.lower() or 'snomed' in system.lower():
            score = 0.9  # Actively maintained
        
        # CPT is annually updated
        elif 'cpt' in system.lower():
            score = 0.8  # Regularly updated
        
        # RxNorm is frequently updated
        elif 'rxnorm' in system.lower():
            score = 0.9  # Frequently updated for medications
        
        return score
    
    def _score_context_relevance(self, code: str, system: str, context: ClinicalContext) -> float:
        """Score context relevance for the clinical domain (0.0 to 1.0)."""
        # Find matching code system
        for system_name, config in self.code_system_patterns.items():
            if re.search(config['pattern'], system, re.IGNORECASE):
                return config['context_weights'].get(context, 0.5)
        
        # Default context scoring for unknown systems
        return 0.5
    
    def _score_validity(self, code: str, system: str) -> float:
        """Score code validity based on validation results (0.0 to 1.0)."""
        if not self.code_validator:
            return 0.5  # Unknown validity without validator
        
        try:
            validation_result = self.code_validator.validate_code(code, system)
            
            if validation_result.result == ValidationResult.VALID:
                return 1.0
            elif validation_result.result == ValidationResult.INVALID:
                return 0.0
            elif validation_result.result == ValidationResult.NOT_FOUND:
                return 0.2  # Code system exists but code not found
            else:  # UNKNOWN
                return 0.5  # Cannot determine validity
                
        except Exception as e:
            self.logger.warning(f"Validation failed for {system}|{code}: {e}")
            return 0.5
    
    def _categorize_specificity(self, code: str, system: str, specificity_score: float) -> SpecificityLevel:
        """Categorize specificity level based on score and code patterns."""
        
        # Check for unspecified patterns first
        # Only .9 or .90 at end indicates unspecified, not other patterns like .50
        if re.search(r'[Uu]nspecified|NOS|\.9$|\.90$', code):
            return SpecificityLevel.UNSPECIFIED
        
        # Score-based categorization
        if specificity_score >= 0.8:
            return SpecificityLevel.VERY_SPECIFIC
        elif specificity_score >= 0.7:
            return SpecificityLevel.SPECIFIC
        elif specificity_score >= 0.5:
            return SpecificityLevel.MODERATE
        else:
            return SpecificityLevel.GENERAL
    
    def _identify_issues(self, code: str, system: str, context: ClinicalContext,
                        specificity_score: float, currency_score: float, 
                        context_score: float, validity_score: float) -> List[str]:
        """Identify appropriateness issues."""
        issues = []
        
        # Specificity issues
        if specificity_score < 0.4:
            issues.append("Code lacks specificity for optimal clinical documentation")
        if re.search(r'[Uu]nspecified|NOS|\.9$|\.90$', code):
            issues.append("Code is unspecified; consider more specific alternative")
        
        # Currency issues  
        if currency_score < 0.5:
            issues.append("Code system may be outdated; consider modern alternatives")
        
        # Context relevance issues
        if context_score < 0.6:
            issues.append(f"Code system not optimal for {context.value} context")
        
        # Validity issues
        if validity_score < 0.5:
            issues.append("Code validity could not be confirmed")
        elif validity_score == 0.0:
            issues.append("Code is invalid in the specified system")
        
        return issues
    
    def _generate_recommendations(self, code: str, system: str, context: ClinicalContext, 
                                 issues: List[str]) -> List[str]:
        """Generate recommendations for improving code appropriateness."""
        recommendations = []
        
        # Specificity recommendations
        if any("specificity" in issue.lower() for issue in issues):
            recommendations.append("Consider using more specific codes with additional clinical details")
            if 'icd-10-cm' in system.lower():
                recommendations.append("Add laterality, anatomical detail, or complication codes if applicable")
        
        if any("unspecified" in issue.lower() for issue in issues):
            recommendations.append("Review clinical documentation for additional details to support more specific coding")
        
        # Context recommendations
        if any("not optimal" in issue.lower() for issue in issues):
            context_suggestions = {
                ClinicalContext.LABORATORY: "Consider LOINC codes for laboratory tests and observations",
                ClinicalContext.MEDICATION: "Consider RxNorm codes for medications",
                ClinicalContext.PROCEDURE: "Consider CPT or ICD-10-PCS codes for procedures",
                ClinicalContext.DIAGNOSIS: "Consider ICD-10-CM or SNOMED CT codes for diagnoses"
            }
            if context in context_suggestions:
                recommendations.append(context_suggestions[context])
        
        # Currency recommendations
        if any("outdated" in issue.lower() for issue in issues):
            recommendations.append("Consider migrating to current coding standards (ICD-10, SNOMED CT)")
        
        # Validity recommendations
        if any("validity" in issue.lower() for issue in issues):
            recommendations.append("Verify code exists in the specified code system")
            recommendations.append("Check for code system URL accuracy and version compatibility")
        
        return recommendations


# Factory function for creating scorers
def create_appropriateness_scorer(scorer_type: str = "default", 
                                code_validator: Optional[CodeValidator] = None,
                                **kwargs) -> CodeAppropriatenessScorer:
    """
    Create a code appropriateness scorer instance.
    
    Args:
        scorer_type: Type of scorer ('default')
        code_validator: Optional code validator for validity scoring
        **kwargs: Additional arguments for scorer constructor
        
    Returns:
        CodeAppropriatenessScorer instance
    """
    if scorer_type.lower() == "default":
        return DefaultAppropriatenessScorer(code_validator=code_validator, **kwargs)
    else:
        raise ValueError(f"Unknown scorer type: {scorer_type}")


# Convenience function for quick scoring
def score_code_appropriateness(code: str, system: str, 
                              clinical_context: ClinicalContext = ClinicalContext.GENERAL,
                              code_validator: Optional[CodeValidator] = None,
                              weights: Optional[Dict[str, float]] = None) -> AppropriatenessScore:
    """
    Quick function to score a single code's appropriateness.
    
    Args:
        code: Code to score
        system: Code system URI
        clinical_context: Clinical context for scoring
        code_validator: Optional validator for validity scoring
        weights: Optional custom scoring weights
        
    Returns:
        AppropriatenessScore with detailed scoring results
    """
    scorer = DefaultAppropriatenessScorer(code_validator=code_validator, weights=weights)
    return scorer.score_code(code, system, clinical_context)