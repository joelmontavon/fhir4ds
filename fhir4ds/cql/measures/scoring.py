"""
CQL Quality Measure Scoring - Measure scoring algorithms and calculations.

This module provides scoring algorithms for different types of quality measures
including proportion, ratio, continuous variable, and cohort measures.
"""

import logging
from typing import Dict, Any, Optional, List, Union, Tuple
from enum import Enum
from datetime import datetime, date
import statistics

logger = logging.getLogger(__name__)

class ScoringMethod(Enum):
    """Quality measure scoring methods."""
    PROPORTION = "proportion"
    RATIO = "ratio" 
    CONTINUOUS_VARIABLE = "continuous-variable"
    COHORT = "cohort"

class MeasureScoring:
    """
    Quality measure scoring engine.
    
    Provides algorithms for calculating measure scores based on different
    scoring methodologies defined in quality measure specifications.
    """
    
    def __init__(self):
        self.scoring_algorithms = {
            ScoringMethod.PROPORTION: self._score_proportion_measure,
            ScoringMethod.RATIO: self._score_ratio_measure,
            ScoringMethod.CONTINUOUS_VARIABLE: self._score_continuous_variable_measure,
            ScoringMethod.COHORT: self._score_cohort_measure
        }
    
    def calculate_score(self, population_results: Dict[str, Any], 
                       scoring_method: Union[str, ScoringMethod],
                       measure_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Calculate measure score based on population results.
        
        Args:
            population_results: Results from population evaluation
            scoring_method: Scoring method to use
            measure_config: Additional configuration for scoring
            
        Returns:
            Score calculation results
        """
        if isinstance(scoring_method, str):
            try:
                scoring_method = ScoringMethod(scoring_method.lower())
            except ValueError:
                return {'error': f"Unknown scoring method: {scoring_method}"}
        
        scorer = self.scoring_algorithms.get(scoring_method)
        if not scorer:
            return {'error': f"No scorer available for method: {scoring_method.value}"}
        
        try:
            return scorer(population_results, measure_config or {})
        except Exception as e:
            logger.error(f"Error calculating score: {e}")
            return {'error': f"Scoring calculation failed: {str(e)}"}
    
    def _score_proportion_measure(self, population_results: Dict[str, Any], 
                                 config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Score proportion-based quality measures.
        
        Formula: Numerator / Denominator
        """
        populations = population_results.get('populations', {})
        
        # Get population counts
        numerator = self._get_population_count(populations, 'numerator', 'Numerator')
        denominator = self._get_population_count(populations, 'denominator', 'Denominator')
        initial_pop = self._get_population_count(populations, 'initial_population', 'Initial Population')
        
        # Get exclusions
        denom_exclusions = self._get_population_count(populations, 'denominator_exclusion', 'Denominator Exclusion')
        denom_exceptions = self._get_population_count(populations, 'denominator_exception', 'Denominator Exception')
        num_exclusions = self._get_population_count(populations, 'numerator_exclusion', 'Numerator Exclusion')
        
        # Calculate adjusted counts
        adjusted_denominator = denominator - denom_exclusions - denom_exceptions
        adjusted_numerator = numerator - num_exclusions
        
        # Calculate score
        if adjusted_denominator == 0:
            score = None
            percentage = None
        else:
            score = adjusted_numerator / adjusted_denominator
            percentage = score * 100
        
        result = {
            'scoring_method': 'proportion',
            'score': score,
            'percentage': percentage,
            'populations': {
                'initial_population': initial_pop,
                'denominator': denominator,
                'denominator_exclusions': denom_exclusions,
                'denominator_exceptions': denom_exceptions,
                'adjusted_denominator': adjusted_denominator,
                'numerator': numerator,
                'numerator_exclusions': num_exclusions,
                'adjusted_numerator': adjusted_numerator
            },
            'performance_rate': percentage,
            'eligible_population': adjusted_denominator
        }
        
        # Add confidence interval if requested
        if config.get('calculate_confidence_interval', False):
            ci = self._calculate_proportion_confidence_interval(
                adjusted_numerator, adjusted_denominator, config.get('confidence_level', 0.95)
            )
            result['confidence_interval'] = ci
        
        return result
    
    def _score_ratio_measure(self, population_results: Dict[str, Any], 
                            config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Score ratio-based quality measures.
        
        Formula: Numerator / Denominator (similar to proportion but different populations)
        """
        populations = population_results.get('populations', {})
        
        # For ratio measures, numerator and denominator may be different concepts
        numerator = self._get_population_count(populations, 'numerator', 'Numerator')
        denominator = self._get_population_count(populations, 'denominator', 'Denominator')
        
        # Calculate ratio
        if denominator == 0:
            ratio = None
        else:
            ratio = numerator / denominator
        
        return {
            'scoring_method': 'ratio',
            'ratio': ratio,
            'numerator': numerator,
            'denominator': denominator,
            'populations': populations
        }
    
    def _score_continuous_variable_measure(self, population_results: Dict[str, Any], 
                                          config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Score continuous variable quality measures.
        
        Calculates statistics on continuous observations.
        """
        populations = population_results.get('populations', {})
        
        # Get measure population and observations
        measure_pop = self._get_population_count(populations, 'measure_population', 'Measure Population')
        
        # For continuous variables, we need the actual observation values
        # This would typically come from measure observations
        observations = population_results.get('observations', [])
        
        if not observations:
            return {
                'scoring_method': 'continuous-variable',
                'error': 'No observations available for continuous variable measure'
            }
        
        # Calculate statistics
        try:
            values = [obs for obs in observations if obs is not None]
            
            if not values:
                return {
                    'scoring_method': 'continuous-variable',
                    'error': 'No valid observation values'
                }
            
            result = {
                'scoring_method': 'continuous-variable',
                'measure_population': measure_pop,
                'observation_count': len(values),
                'mean': statistics.mean(values),
                'median': statistics.median(values),
                'min': min(values),
                'max': max(values),
                'sum': sum(values)
            }
            
            # Add standard deviation if more than one value
            if len(values) > 1:
                result['standard_deviation'] = statistics.stdev(values)
                result['variance'] = statistics.variance(values)
            
            # Add percentiles if requested
            if config.get('calculate_percentiles', False):
                sorted_values = sorted(values)
                n = len(sorted_values)
                result['percentiles'] = {
                    '25th': sorted_values[int(n * 0.25)],
                    '75th': sorted_values[int(n * 0.75)],
                    '90th': sorted_values[int(n * 0.90)],
                    '95th': sorted_values[int(n * 0.95)]
                }
            
            return result
            
        except Exception as e:
            return {
                'scoring_method': 'continuous-variable',
                'error': f'Statistics calculation failed: {str(e)}'
            }
    
    def _score_cohort_measure(self, population_results: Dict[str, Any], 
                             config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Score cohort quality measures.
        
        Simply reports the count of patients meeting criteria.
        """
        populations = population_results.get('populations', {})
        
        # Get initial population count
        initial_pop = self._get_population_count(populations, 'initial_population', 'Initial Population')
        
        return {
            'scoring_method': 'cohort',
            'cohort_count': initial_pop,
            'populations': populations
        }
    
    def _get_population_count(self, populations: Dict[str, Any], 
                             key: str, alt_key: str = None) -> int:
        """
        Get population count, trying multiple key variations.
        
        Args:
            populations: Population results dictionary
            key: Primary key to look for
            alt_key: Alternative key name
            
        Returns:
            Population count (0 if not found)
        """
        # Try exact key first
        if key in populations:
            pop_data = populations[key]
            return pop_data.get('count', 0) if isinstance(pop_data, dict) else 0
        
        # Try alternative key
        if alt_key and alt_key in populations:
            pop_data = populations[alt_key]
            return pop_data.get('count', 0) if isinstance(pop_data, dict) else 0
        
        # Try case-insensitive search
        for pop_key, pop_data in populations.items():
            if pop_key.lower().replace(' ', '_') == key.lower():
                return pop_data.get('count', 0) if isinstance(pop_data, dict) else 0
        
        return 0
    
    def _calculate_proportion_confidence_interval(self, numerator: int, denominator: int, 
                                                 confidence_level: float = 0.95) -> Dict[str, float]:
        """
        Calculate confidence interval for proportion measures.
        
        Uses Wilson score interval method.
        """
        if denominator == 0:
            return {'lower': 0, 'upper': 0, 'method': 'wilson'}
        
        # Import here to avoid dependency issues
        import math
        
        p = numerator / denominator
        n = denominator
        
        # Z-score for confidence level
        z_scores = {0.90: 1.645, 0.95: 1.96, 0.99: 2.576}
        z = z_scores.get(confidence_level, 1.96)
        
        # Wilson score interval
        center = (p + z**2/(2*n)) / (1 + z**2/n)
        margin = z * math.sqrt((p*(1-p) + z**2/(4*n)) / n) / (1 + z**2/n)
        
        lower = max(0, center - margin)
        upper = min(1, center + margin)
        
        return {
            'lower': lower,
            'upper': upper,
            'lower_percentage': lower * 100,
            'upper_percentage': upper * 100,
            'confidence_level': confidence_level,
            'method': 'wilson'
        }

class MeasureReport:
    """
    Quality measure report generator.
    
    Generates comprehensive reports from measure evaluation results.
    """
    
    def __init__(self):
        pass
    
    def generate_report(self, measure_id: str, measure_title: str, 
                       evaluation_results: Dict[str, Any], 
                       scoring_results: Dict[str, Any],
                       report_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Generate comprehensive measure report.
        
        Args:
            measure_id: Measure identifier
            measure_title: Measure title
            evaluation_results: Population evaluation results
            scoring_results: Scoring calculation results
            report_config: Report configuration options
            
        Returns:
            Comprehensive measure report
        """
        config = report_config or {}
        
        report = {
            'measure_id': measure_id,
            'measure_title': measure_title,
            'report_generated': datetime.now().isoformat(),
            'evaluation_type': evaluation_results.get('evaluation_type', 'unknown'),
            'scoring_method': scoring_results.get('scoring_method', 'unknown'),
            'performance': self._extract_performance_data(scoring_results),
            'populations': self._format_population_data(evaluation_results.get('populations', {})),
            'summary': self._generate_summary(scoring_results)
        }
        
        # Add patient-level details if available and requested
        if config.get('include_patient_details', False) and 'patient_results' in evaluation_results:
            report['patient_details'] = self._format_patient_details(
                evaluation_results['patient_results']
            )
        
        # Add stratification if available
        if 'stratifiers' in evaluation_results:
            report['stratification'] = evaluation_results['stratifiers']
        
        return report
    
    def _extract_performance_data(self, scoring_results: Dict[str, Any]) -> Dict[str, Any]:
        """Extract key performance metrics from scoring results."""
        scoring_method = scoring_results.get('scoring_method')
        
        if scoring_method == 'proportion':
            return {
                'score': scoring_results.get('score'),
                'percentage': scoring_results.get('percentage'),
                'performance_rate': scoring_results.get('performance_rate'),
                'eligible_population': scoring_results.get('eligible_population'),
                'confidence_interval': scoring_results.get('confidence_interval')
            }
        elif scoring_method == 'continuous-variable':
            return {
                'mean': scoring_results.get('mean'),
                'median': scoring_results.get('median'),
                'min': scoring_results.get('min'),
                'max': scoring_results.get('max'),
                'standard_deviation': scoring_results.get('standard_deviation'),
                'observation_count': scoring_results.get('observation_count')
            }
        elif scoring_method == 'cohort':
            return {
                'cohort_count': scoring_results.get('cohort_count')
            }
        else:
            return scoring_results
    
    def _format_population_data(self, populations: Dict[str, Any]) -> Dict[str, Any]:
        """Format population data for reporting."""
        formatted = {}
        
        for pop_name, pop_data in populations.items():
            if isinstance(pop_data, dict):
                formatted[pop_name] = {
                    'count': pop_data.get('count', 0),
                    'description': pop_data.get('description', ''),
                    'type': pop_data.get('type', '')
                }
                
                # Include patient list if available and not too large
                patients = pop_data.get('patients', [])
                if patients and len(patients) <= 100:  # Limit patient list size
                    formatted[pop_name]['patient_ids'] = patients
                elif patients:
                    formatted[pop_name]['patient_count'] = len(patients)
                    formatted[pop_name]['sample_patients'] = patients[:10]
        
        return formatted
    
    def _generate_summary(self, scoring_results: Dict[str, Any]) -> str:
        """Generate text summary of measure results."""
        scoring_method = scoring_results.get('scoring_method')
        
        if scoring_method == 'proportion':
            percentage = scoring_results.get('percentage')
            denominator = scoring_results.get('populations', {}).get('adjusted_denominator', 0)
            numerator = scoring_results.get('populations', {}).get('adjusted_numerator', 0)
            
            if percentage is not None:
                return f"Performance rate: {percentage:.1f}% ({numerator} of {denominator} patients)"
            else:
                return "Unable to calculate performance rate (no eligible patients)"
        
        elif scoring_method == 'continuous-variable':
            mean = scoring_results.get('mean')
            count = scoring_results.get('observation_count', 0)
            
            if mean is not None:
                return f"Average value: {mean:.2f} (based on {count} observations)"
            else:
                return "Unable to calculate average (no observations)"
        
        elif scoring_method == 'cohort':
            count = scoring_results.get('cohort_count', 0)
            return f"Cohort size: {count} patients"
        
        else:
            return f"Results calculated using {scoring_method} scoring method"
    
    def _format_patient_details(self, patient_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Format patient-level results for reporting."""
        details = []
        
        for patient_id, results in patient_results.items():
            detail = {
                'patient_id': patient_id,
                'populations': results
            }
            details.append(detail)
        
        return details