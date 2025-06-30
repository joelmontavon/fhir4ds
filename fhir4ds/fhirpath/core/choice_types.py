"""
FHIR Choice Type Mappings

This module provides comprehensive FHIR R4 choice type mappings for the ofType() function.
"""

import json
import os
from typing import Dict, Optional, List

# Generated from choiceTypePaths.json - 187 mappings
# Comprehensive FHIR R4 choice type mappings for production reliability
COMPREHENSIVE_CHOICE_MAPPINGS = {
    "ActivityDefinition.product": ['Reference', 'CodeableConcept'],
    "ActivityDefinition.subject": ['CodeableConcept', 'Reference'],
    "ActivityDefinition.timing": ['Timing', 'DateTime', 'Age', 'Period', 'Range', 'Duration'],
    "AllergyIntolerance.onset": ['DateTime', 'Age', 'Period', 'Range', 'String'],
    "Annotation.author": ['Reference', 'String'],
    "AuditEvent.entity.detail.value": ['String', 'Base64Binary'],
    "BiologicallyDerivedProduct.collection.collected": ['DateTime', 'Period'],
    "BiologicallyDerivedProduct.manipulation.time": ['DateTime', 'Period'],
    "BiologicallyDerivedProduct.processing.time": ['DateTime', 'Period'],
    "CarePlan.activity.detail.product": ['CodeableConcept', 'Reference'],
    "CarePlan.activity.detail.scheduled": ['Timing', 'Period', 'String'],
    "ChargeItem.occurrence": ['DateTime', 'Period', 'Timing'],
    "ChargeItem.product": ['Reference', 'CodeableConcept'],
    "Claim.accident.location": ['Address', 'Reference'],
    "Claim.diagnosis.diagnosis": ['CodeableConcept', 'Reference'],
    "Claim.item.location": ['CodeableConcept', 'Address', 'Reference'],
    "Claim.item.serviced": ['Date', 'Period'],
    "Claim.procedure.procedure": ['CodeableConcept', 'Reference'],
    "Claim.supportingInfo.timing": ['Date', 'Period'],
    "Claim.supportingInfo.value": ['Boolean', 'String', 'Quantity', 'Attachment', 'Reference'],
    "ClaimResponse.addItem.location": ['CodeableConcept', 'Address', 'Reference'],
    "ClaimResponse.addItem.serviced": ['Date', 'Period'],
    "ClinicalImpression.effective": ['DateTime', 'Period'],
    "CodeSystem.concept.property.value": ['Code', 'Coding', 'String', 'Integer', 'Boolean', 'DateTime', 'Decimal'],
    "Communication.payload.content": ['String', 'Attachment', 'Reference'],
    "CommunicationRequest.occurrence": ['DateTime', 'Period'],
    "CommunicationRequest.payload.content": ['String', 'Attachment', 'Reference'],
    "Composition.relatesTo.target": ['Identifier', 'Reference'],
    "ConceptMap.source": ['Uri', 'Canonical'],
    "ConceptMap.target": ['Uri', 'Canonical'],
    "Condition.abatement": ['DateTime', 'Age', 'Period', 'Range', 'String'],
    "Condition.onset": ['DateTime', 'Age', 'Period', 'Range', 'String'],
    "Consent.source": ['Attachment', 'Reference'],
    "Contract.friendly.content": ['Attachment', 'Reference'],
    "Contract.legal.content": ['Attachment', 'Reference'],
    "Contract.legallyBinding": ['Attachment', 'Reference'],
    "Contract.rule.content": ['Attachment', 'Reference'],
    "Contract.term.action.occurrence": ['DateTime', 'Period', 'Timing'],
    "Contract.term.asset.valuedItem.entity": ['CodeableConcept', 'Reference'],
    "Contract.term.offer.answer.value": ['Boolean', 'Decimal', 'Integer', 'Date', 'DateTime', 'Time', 'String', 'Uri', 'Attachment', 'Coding', 'Quantity', 'Reference'],
    "Contract.term.topic": ['CodeableConcept', 'Reference'],
    "Contract.topic": ['CodeableConcept', 'Reference'],
    "Coverage.costToBeneficiary.value": ['Quantity', 'Money'],
    "CoverageEligibilityRequest.item.diagnosis.diagnosis": ['CodeableConcept', 'Reference'],
    "CoverageEligibilityRequest.serviced": ['Date', 'Period'],
    "CoverageEligibilityResponse.insurance.item.benefit.allowed": ['UnsignedInt', 'String', 'Money'],
    "CoverageEligibilityResponse.insurance.item.benefit.used": ['UnsignedInt', 'String', 'Money'],
    "CoverageEligibilityResponse.serviced": ['Date', 'Period'],
    "DataRequirement.dateFilter.value": ['DateTime', 'Period', 'Duration'],
    "DataRequirement.subject": ['CodeableConcept', 'Reference'],
    "DetectedIssue.identified": ['DateTime', 'Period'],
    "DeviceDefinition.manufacturer": ['String', 'Reference'],
    "DeviceRequest.code": ['Reference', 'CodeableConcept'],
    "DeviceRequest.occurrence": ['DateTime', 'Period', 'Timing'],
    "DeviceRequest.parameter.value": ['CodeableConcept', 'Quantity', 'Range', 'Boolean'],
    "DeviceUseStatement.timing": ['Timing', 'Period', 'DateTime'],
    "DiagnosticReport.effective": ['DateTime', 'Period'],
    "Dosage.asNeeded": ['Boolean', 'CodeableConcept'],
    "Dosage.doseAndRate.dose": ['Range', 'Quantity'],
    "Dosage.doseAndRate.rate": ['Ratio', 'Range', 'Quantity'],
    "ElementDefinition.defaultValue": ['Base64Binary', 'Boolean', 'Canonical', 'Code', 'Date', 'DateTime', 'Decimal', 'Id', 'Instant', 'Integer', 'Markdown', 'Oid', 'PositiveInt', 'String', 'Time', 'UnsignedInt', 'Uri', 'Url', 'Uuid', 'Address', 'Age', 'Annotation', 'Attachment', 'CodeableConcept', 'Coding', 'ContactPoint', 'Count', 'Distance', 'Duration', 'HumanName', 'Identifier', 'Money', 'Period', 'Quantity', 'Range', 'Ratio', 'Reference', 'SampledData', 'Signature', 'Timing', 'ContactDetail', 'Contributor', 'DataRequirement', 'Expression', 'ParameterDefinition', 'RelatedArtifact', 'TriggerDefinition', 'UsageContext', 'Dosage', 'Meta'],
    "ElementDefinition.example.value": ['Base64Binary', 'Boolean', 'Canonical', 'Code', 'Date', 'DateTime', 'Decimal', 'Id', 'Instant', 'Integer', 'Markdown', 'Oid', 'PositiveInt', 'String', 'Time', 'UnsignedInt', 'Uri', 'Url', 'Uuid', 'Address', 'Age', 'Annotation', 'Attachment', 'CodeableConcept', 'Coding', 'ContactPoint', 'Count', 'Distance', 'Duration', 'HumanName', 'Identifier', 'Money', 'Period', 'Quantity', 'Range', 'Ratio', 'Reference', 'SampledData', 'Signature', 'Timing', 'ContactDetail', 'Contributor', 'DataRequirement', 'Expression', 'ParameterDefinition', 'RelatedArtifact', 'TriggerDefinition', 'UsageContext', 'Dosage', 'Meta'],
    "ElementDefinition.extension.value": ['CodeableConcept', 'Canonical'],
    "ElementDefinition.fixed": ['Base64Binary', 'Boolean', 'Canonical', 'Code', 'Date', 'DateTime', 'Decimal', 'Id', 'Instant', 'Integer', 'Markdown', 'Oid', 'PositiveInt', 'String', 'Time', 'UnsignedInt', 'Uri', 'Url', 'Uuid', 'Address', 'Age', 'Annotation', 'Attachment', 'CodeableConcept', 'Coding', 'ContactPoint', 'Count', 'Distance', 'Duration', 'HumanName', 'Identifier', 'Money', 'Period', 'Quantity', 'Range', 'Ratio', 'Reference', 'SampledData', 'Signature', 'Timing', 'ContactDetail', 'Contributor', 'DataRequirement', 'Expression', 'ParameterDefinition', 'RelatedArtifact', 'TriggerDefinition', 'UsageContext', 'Dosage', 'Meta'],
    "ElementDefinition.maxValue": ['Date', 'DateTime', 'Instant', 'Time', 'Decimal', 'Integer', 'PositiveInt', 'UnsignedInt', 'Quantity'],
    "ElementDefinition.minValue": ['Date', 'DateTime', 'Instant', 'Time', 'Decimal', 'Integer', 'PositiveInt', 'UnsignedInt', 'Quantity'],
    "ElementDefinition.pattern": ['Base64Binary', 'Boolean', 'Canonical', 'Code', 'Date', 'DateTime', 'Decimal', 'Id', 'Instant', 'Integer', 'Markdown', 'Oid', 'PositiveInt', 'String', 'Time', 'UnsignedInt', 'Uri', 'Url', 'Uuid', 'Address', 'Age', 'Annotation', 'Attachment', 'CodeableConcept', 'Coding', 'ContactPoint', 'Count', 'Distance', 'Duration', 'HumanName', 'Identifier', 'Money', 'Period', 'Quantity', 'Range', 'Ratio', 'Reference', 'SampledData', 'Signature', 'Timing', 'ContactDetail', 'Contributor', 'DataRequirement', 'Expression', 'ParameterDefinition', 'RelatedArtifact', 'TriggerDefinition', 'UsageContext', 'Dosage', 'Meta'],
    "EventDefinition.subject": ['CodeableConcept', 'Reference'],
    "EvidenceVariable.characteristic.definition": ['Reference', 'Canonical', 'CodeableConcept', 'Expression', 'DataRequirement', 'TriggerDefinition'],
    "EvidenceVariable.characteristic.participantEffective": ['DateTime', 'Period', 'Duration', 'Timing'],
    "ExplanationOfBenefit.accident.location": ['Address', 'Reference'],
    "ExplanationOfBenefit.addItem.location": ['CodeableConcept', 'Address', 'Reference'],
    "ExplanationOfBenefit.addItem.serviced": ['Date', 'Period'],
    "ExplanationOfBenefit.benefitBalance.financial.allowed": ['UnsignedInt', 'String', 'Money'],
    "ExplanationOfBenefit.benefitBalance.financial.used": ['UnsignedInt', 'Money'],
    "ExplanationOfBenefit.diagnosis.diagnosis": ['CodeableConcept', 'Reference'],
    "ExplanationOfBenefit.item.location": ['CodeableConcept', 'Address', 'Reference'],
    "ExplanationOfBenefit.item.serviced": ['Date', 'Period'],
    "ExplanationOfBenefit.procedure.procedure": ['CodeableConcept', 'Reference'],
    "ExplanationOfBenefit.supportingInfo.timing": ['Date', 'Period'],
    "ExplanationOfBenefit.supportingInfo.value": ['Boolean', 'String', 'Quantity', 'Attachment', 'Reference'],
    "Extension.value": ['Base64Binary', 'Boolean', 'Canonical', 'Code', 'Date', 'DateTime', 'Decimal', 'Id', 'Instant', 'Integer', 'Markdown', 'Oid', 'PositiveInt', 'String', 'Time', 'UnsignedInt', 'Uri', 'Url', 'Uuid', 'Address', 'Age', 'Annotation', 'Attachment', 'CodeableConcept', 'Coding', 'ContactPoint', 'Count', 'Distance', 'Duration', 'HumanName', 'Identifier', 'Money', 'Period', 'Quantity', 'Range', 'Ratio', 'Reference', 'SampledData', 'Signature', 'Timing', 'ContactDetail', 'Contributor', 'DataRequirement', 'Expression', 'ParameterDefinition', 'RelatedArtifact', 'TriggerDefinition', 'UsageContext', 'Dosage', 'Meta'],
    "FamilyMemberHistory.age": ['Age', 'Range', 'String'],
    "FamilyMemberHistory.born": ['Period', 'Date', 'String'],
    "FamilyMemberHistory.condition.onset": ['Age', 'Range', 'Period', 'String'],
    "FamilyMemberHistory.deceased": ['Boolean', 'Age', 'Range', 'Date', 'String'],
    "Goal.start": ['Date', 'CodeableConcept'],
    "Goal.target.detail": ['Quantity', 'Range', 'CodeableConcept', 'String', 'Boolean', 'Integer', 'Ratio'],
    "Goal.target.due": ['Date', 'Duration'],
    "Group.characteristic.value": ['CodeableConcept', 'Boolean', 'Quantity', 'Range', 'Reference'],
    "GuidanceResponse.module": ['Uri', 'Canonical', 'CodeableConcept'],
    "Immunization.occurrence": ['DateTime', 'String'],
    "Immunization.protocolApplied.doseNumber": ['PositiveInt', 'String'],
    "Immunization.protocolApplied.seriesDoses": ['PositiveInt', 'String'],
    "ImmunizationEvaluation.doseNumber": ['PositiveInt', 'String'],
    "ImmunizationEvaluation.seriesDoses": ['PositiveInt', 'String'],
    "ImmunizationRecommendation.recommendation.doseNumber": ['PositiveInt', 'String'],
    "ImmunizationRecommendation.recommendation.seriesDoses": ['PositiveInt', 'String'],
    "ImplementationGuide.definition.page.name": ['Url', 'Reference'],
    "ImplementationGuide.definition.resource.example": ['Boolean', 'Canonical'],
    "ImplementationGuide.manifest.resource.example": ['Boolean', 'Canonical'],
    "Invoice.lineItem.chargeItem": ['Reference', 'CodeableConcept'],
    "Library.subject": ['CodeableConcept', 'Reference'],
    "Measure.subject": ['CodeableConcept', 'Reference'],
    "Media.created": ['DateTime', 'Period'],
    "Medication.ingredient.item": ['CodeableConcept', 'Reference'],
    "MedicationAdministration.dosage.rate": ['Ratio', 'Quantity'],
    "MedicationAdministration.effective": ['DateTime', 'Period'],
    "MedicationAdministration.medication": ['CodeableConcept', 'Reference'],
    "MedicationDispense.medication": ['CodeableConcept', 'Reference'],
    "MedicationDispense.statusReason": ['CodeableConcept', 'Reference'],
    "MedicationKnowledge.administrationGuidelines.indication": ['CodeableConcept', 'Reference'],
    "MedicationKnowledge.administrationGuidelines.patientCharacteristics.characteristic": ['CodeableConcept', 'Quantity'],
    "MedicationKnowledge.drugCharacteristic.value": ['CodeableConcept', 'String', 'Quantity', 'Base64Binary'],
    "MedicationKnowledge.ingredient.item": ['CodeableConcept', 'Reference'],
    "MedicationRequest.medication": ['CodeableConcept', 'Reference'],
    "MedicationRequest.reported": ['Boolean', 'Reference'],
    "MedicationRequest.substitution.allowed": ['Boolean', 'CodeableConcept'],
    "MedicationStatement.effective": ['DateTime', 'Period'],
    "MedicationStatement.medication": ['CodeableConcept', 'Reference'],
    "MedicinalProduct.specialDesignation.indication": ['CodeableConcept', 'Reference'],
    "MedicinalProductAuthorization.procedure.date": ['Period', 'DateTime'],
    "MedicinalProductContraindication.otherTherapy.medication": ['CodeableConcept', 'Reference'],
    "MedicinalProductIndication.otherTherapy.medication": ['CodeableConcept', 'Reference'],
    "MedicinalProductInteraction.interactant.item": ['Reference', 'CodeableConcept'],
    "MessageDefinition.event": ['Coding', 'Uri'],
    "MessageHeader.event": ['Coding', 'Uri'],
    "NutritionOrder.enteralFormula.administration.rate": ['Quantity', 'Ratio'],
    "Observation.component.value": ['Quantity', 'CodeableConcept', 'String', 'Boolean', 'Integer', 'Range', 'Ratio', 'SampledData', 'Time', 'DateTime', 'Period'],
    "Observation.effective": ['DateTime', 'Period', 'Timing', 'Instant'],
    "Observation.value": ['Quantity', 'CodeableConcept', 'String', 'Boolean', 'Integer', 'Range', 'Ratio', 'SampledData', 'Time', 'DateTime', 'Period'],
    "Parameters.parameter.value": ['Base64Binary', 'Boolean', 'Canonical', 'Code', 'Date', 'DateTime', 'Decimal', 'Id', 'Instant', 'Integer', 'Markdown', 'Oid', 'PositiveInt', 'String', 'Time', 'UnsignedInt', 'Uri', 'Url', 'Uuid', 'Address', 'Age', 'Annotation', 'Attachment', 'CodeableConcept', 'Coding', 'ContactPoint', 'Count', 'Distance', 'Duration', 'HumanName', 'Identifier', 'Money', 'Period', 'Quantity', 'Range', 'Ratio', 'Reference', 'SampledData', 'Signature', 'Timing', 'ContactDetail', 'Contributor', 'DataRequirement', 'Expression', 'ParameterDefinition', 'RelatedArtifact', 'TriggerDefinition', 'UsageContext', 'Dosage', 'Meta'],
    "Patient.deceased": ['Boolean', 'DateTime'],
    "Patient.multipleBirth": ['Boolean', 'Integer'],
    "PlanDefinition.action.definition": ['Canonical', 'Uri'],
    "PlanDefinition.action.relatedAction.offset": ['Duration', 'Range'],
    "PlanDefinition.action.subject": ['CodeableConcept', 'Reference'],
    "PlanDefinition.action.timing": ['DateTime', 'Age', 'Period', 'Duration', 'Range', 'Timing'],
    "PlanDefinition.goal.target.detail": ['Quantity', 'Range', 'CodeableConcept'],
    "PlanDefinition.subject": ['CodeableConcept', 'Reference'],
    "Population.age": ['Range', 'CodeableConcept'],
    "Procedure.performed": ['DateTime', 'Period', 'String', 'Age', 'Range'],
    "Provenance.occurred": ['Period', 'DateTime'],
    "Questionnaire.item.answerOption.value": ['Integer', 'Date', 'Time', 'String', 'Coding', 'Reference'],
    "Questionnaire.item.enableWhen.answer": ['Boolean', 'Decimal', 'Integer', 'Date', 'DateTime', 'Time', 'String', 'Coding', 'Quantity', 'Reference'],
    "Questionnaire.item.initial.value": ['Boolean', 'Decimal', 'Integer', 'Date', 'DateTime', 'Time', 'String', 'Uri', 'Attachment', 'Coding', 'Quantity', 'Reference'],
    "QuestionnaireResponse.item.answer.value": ['Boolean', 'Decimal', 'Integer', 'Date', 'DateTime', 'Time', 'String', 'Uri', 'Attachment', 'Coding', 'Quantity', 'Reference'],
    "RequestGroup.action.relatedAction.offset": ['Duration', 'Range'],
    "RequestGroup.action.timing": ['DateTime', 'Age', 'Period', 'Duration', 'Range', 'Timing'],
    "ResearchDefinition.subject": ['CodeableConcept', 'Reference'],
    "ResearchElementDefinition.characteristic.definition": ['CodeableConcept', 'Canonical', 'Expression', 'DataRequirement'],
    "ResearchElementDefinition.characteristic.participantEffective": ['DateTime', 'Period', 'Duration', 'Timing'],
    "ResearchElementDefinition.characteristic.studyEffective": ['DateTime', 'Period', 'Duration', 'Timing'],
    "ResearchElementDefinition.subject": ['CodeableConcept', 'Reference'],
    "RiskAssessment.occurrence": ['DateTime', 'Period'],
    "RiskAssessment.prediction.probability": ['Decimal', 'Range'],
    "RiskAssessment.prediction.when": ['Period', 'Range'],
    "ServiceRequest.asNeeded": ['Boolean', 'CodeableConcept'],
    "ServiceRequest.occurrence": ['DateTime', 'Period', 'Timing'],
    "ServiceRequest.quantity": ['Quantity', 'Ratio', 'Range'],
    "Specimen.collection.collected": ['DateTime', 'Period'],
    "Specimen.collection.fastingStatus": ['CodeableConcept', 'Duration'],
    "Specimen.container.additive": ['CodeableConcept', 'Reference'],
    "Specimen.processing.time": ['DateTime', 'Period'],
    "SpecimenDefinition.typeTested.container.additive.additive": ['CodeableConcept', 'Reference'],
    "SpecimenDefinition.typeTested.container.minimumVolume": ['Quantity', 'String'],
    "StructureMap.group.rule.source.defaultValue": ['Base64Binary', 'Boolean', 'Canonical', 'Code', 'Date', 'DateTime', 'Decimal', 'Id', 'Instant', 'Integer', 'Markdown', 'Oid', 'PositiveInt', 'String', 'Time', 'UnsignedInt', 'Uri', 'Url', 'Uuid', 'Address', 'Age', 'Annotation', 'Attachment', 'CodeableConcept', 'Coding', 'ContactPoint', 'Count', 'Distance', 'Duration', 'HumanName', 'Identifier', 'Money', 'Period', 'Quantity', 'Range', 'Ratio', 'Reference', 'SampledData', 'Signature', 'Timing', 'ContactDetail', 'Contributor', 'DataRequirement', 'Expression', 'ParameterDefinition', 'RelatedArtifact', 'TriggerDefinition', 'UsageContext', 'Dosage', 'Meta'],
    "StructureMap.group.rule.target.parameter.value": ['Id', 'String', 'Boolean', 'Integer', 'Decimal'],
    "Substance.ingredient.substance": ['CodeableConcept', 'Reference'],
    "SubstanceAmount.amount": ['Quantity', 'Range', 'String'],
    "SubstanceReferenceInformation.target.amount": ['Quantity', 'Range', 'String'],
    "SubstanceSpecification.moiety.amount": ['Quantity', 'String'],
    "SubstanceSpecification.property.amount": ['Quantity', 'String'],
    "SubstanceSpecification.property.definingSubstance": ['Reference', 'CodeableConcept'],
    "SubstanceSpecification.relationship.amount": ['Quantity', 'Range', 'Ratio', 'String'],
    "SubstanceSpecification.relationship.substance": ['Reference', 'CodeableConcept'],
    "SupplyDelivery.occurrence": ['DateTime', 'Period', 'Timing'],
    "SupplyDelivery.suppliedItem.item": ['CodeableConcept', 'Reference'],
    "SupplyRequest.item": ['CodeableConcept', 'Reference'],
    "SupplyRequest.occurrence": ['DateTime', 'Period', 'Timing'],
    "SupplyRequest.parameter.value": ['CodeableConcept', 'Quantity', 'Range', 'Boolean'],
    "Task.input.value": ['Base64Binary', 'Boolean', 'Canonical', 'Code', 'Date', 'DateTime', 'Decimal', 'Id', 'Instant', 'Integer', 'Markdown', 'Oid', 'PositiveInt', 'String', 'Time', 'UnsignedInt', 'Uri', 'Url', 'Uuid', 'Address', 'Age', 'Annotation', 'Attachment', 'CodeableConcept', 'Coding', 'ContactPoint', 'Count', 'Distance', 'Duration', 'HumanName', 'Identifier', 'Money', 'Period', 'Quantity', 'Range', 'Ratio', 'Reference', 'SampledData', 'Signature', 'Timing', 'ContactDetail', 'Contributor', 'DataRequirement', 'Expression', 'ParameterDefinition', 'RelatedArtifact', 'TriggerDefinition', 'UsageContext', 'Dosage', 'Meta'],
    "Task.output.value": ['Base64Binary', 'Boolean', 'Canonical', 'Code', 'Date', 'DateTime', 'Decimal', 'Id', 'Instant', 'Integer', 'Markdown', 'Oid', 'PositiveInt', 'String', 'Time', 'UnsignedInt', 'Uri', 'Url', 'Uuid', 'Address', 'Age', 'Annotation', 'Attachment', 'CodeableConcept', 'Coding', 'ContactPoint', 'Count', 'Distance', 'Duration', 'HumanName', 'Identifier', 'Money', 'Period', 'Quantity', 'Range', 'Ratio', 'Reference', 'SampledData', 'Signature', 'Timing', 'ContactDetail', 'Contributor', 'DataRequirement', 'Expression', 'ParameterDefinition', 'RelatedArtifact', 'TriggerDefinition', 'UsageContext', 'Dosage', 'Meta'],
    "Timing.repeat.bounds": ['Duration', 'Range', 'Period'],
    "TriggerDefinition.timing": ['Timing', 'Reference', 'Date', 'DateTime'],
    "UsageContext.value": ['CodeableConcept', 'Quantity', 'Range', 'Reference'],
    "ValueSet.expansion.parameter.value": ['String', 'Boolean', 'Integer', 'Decimal', 'Uri', 'Code', 'DateTime'],
}


class FHIRChoiceTypes:
    """Manages FHIR choice type mappings for ofType() function"""
    
    def __init__(self):
        self._choice_mappings = None
        self._load_choice_mappings()
    
    def _load_choice_mappings(self):
        """Load comprehensive FHIR choice type mappings with reliable fallback"""
        try:
            # Try to load from JSON file first (for development/updates)
            choice_types_path = os.path.join(
                os.path.dirname(__file__), 
                'choiceTypePaths.json'
            )
            
            with open(choice_types_path, 'r') as f:
                self._choice_mappings = json.load(f)
                
        except FileNotFoundError:
            # Use comprehensive embedded mappings (production reliability)
            print("Using embedded comprehensive choice type mappings (187 mappings)")
            self._choice_mappings = COMPREHENSIVE_CHOICE_MAPPINGS
    
    
    def get_choice_field_mapping(self, base_path: str, field_name: str, type_name: str) -> Optional[str]:
        """
        Get choice field mapping for a given base path, field, and type.
        
        Args:
            base_path: Base resource path (e.g., "Observation", "Patient")
            field_name: Choice field name (e.g., "value", "deceased") 
            type_name: Target type (e.g., "Quantity", "boolean")
            
        Returns:
            Mapped field name (e.g., "valueQuantity", "deceasedBoolean") or None
        """
        # Try different path combinations
        for path_variant in self._generate_path_variants(base_path, field_name):
            if path_variant in self._choice_mappings:
                choice_types = self._choice_mappings[path_variant]
                if type_name in choice_types:
                    return self._format_choice_field_name(field_name, type_name)
        
        return None
    
    def get_choice_field_mapping_direct(self, field_name: str, type_name: str) -> Optional[str]:
        """
        Direct choice field mapping without base path (for backward compatibility).
        
        Args:
            field_name: Choice field name (e.g., "value", "identified")
            type_name: Target type (e.g., "Quantity", "dateTime")
            
        Returns:
            Mapped field name (e.g., "valueQuantity", "identifiedDateTime") or None
        """
        # Normalize type name for case-insensitive matching
        normalized_type = self._normalize_type_name(type_name)
        
        # Search through all choice mappings for fields ending with the field_name
        for path, types in self._choice_mappings.items():
            path_parts = path.split('.')
            if len(path_parts) >= 2:
                last_part = path_parts[-1]
                if last_part == field_name:
                    # Check for case-insensitive type match
                    for available_type in types:
                        if self._normalize_type_name(available_type) == normalized_type:
                            return self._format_choice_field_name(field_name, available_type)
        
        return None
    
    def _generate_path_variants(self, base_path: str, field_name: str) -> List[str]:
        """Generate possible path variants to search in choice mappings"""
        variants = []
        
        # Direct mapping: "ResourceType.field"
        variants.append(f"{base_path}.{field_name}")
        
        # Common resource variants
        if base_path == "Patient":
            variants.extend([
                f"Patient.{field_name}",
            ])
        elif base_path == "Observation":
            variants.extend([
                f"Observation.{field_name}",
                f"Observation.component.{field_name}"  # For component observations
            ])
        elif base_path == "Condition":
            variants.extend([
                f"Condition.{field_name}",
            ])
        
        return variants
    
    def _normalize_type_name(self, type_name: str) -> str:
        """Normalize type name for case-insensitive matching"""
        return type_name.lower()
    
    def _format_choice_field_name(self, field_name: str, type_name: str) -> str:
        """Format the choice field name following FHIR conventions"""
        # Capitalize the first letter of the type for field name
        formatted_type = type_name[0].upper() + type_name[1:] if type_name else ""
        return f"{field_name}{formatted_type}"
    
    def get_all_choice_types_for_field(self, field_name: str) -> List[str]:
        """Get all possible choice types for a given field name"""
        choice_types = set()
        
        for path, types in self._choice_mappings.items():
            path_parts = path.split('.')
            if len(path_parts) >= 2 and path_parts[-1] == field_name:
                choice_types.update(types)
        
        return list(choice_types)
    
    def get_total_mappings_count(self) -> int:
        """Get total number of choice type mappings available"""
        return len(self._choice_mappings)
    
    def get_mappings_for_resource(self, resource_type: str) -> Dict[str, List[str]]:
        """Get all choice type mappings for a specific resource type"""
        resource_mappings = {}
        
        for path, types in self._choice_mappings.items():
            if path.startswith(f"{resource_type}."):
                resource_mappings[path] = types
        
        return resource_mappings


# Global instance for easy access
fhir_choice_types = FHIRChoiceTypes()