"""
Test Data Generator

Utilities for generating consistent FHIR test data for Phase 2 integration tests.
"""

import json
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta


def generate_test_patients(count: int = 3) -> List[Dict[str, Any]]:
    """
    Generate test Patient resources.
    
    Args:
        count: Number of patients to generate
        
    Returns:
        List of Patient resource dictionaries
    """
    patients = []
    
    base_patients = [
        {
            "id": "patient-001",
            "family": "Smith",
            "given": ["John", "David"],
            "gender": "male",
            "birth_date": "1985-03-15",
            "phone": "555-1234",
            "email": "john.smith@example.com",
            "city": "Anytown",
            "state": "CA"
        },
        {
            "id": "patient-002", 
            "family": "Johnson",
            "given": ["Sarah", "Marie"],
            "gender": "female",
            "birth_date": "1992-07-22",
            "phone": "555-5678",
            "email": "sarah.johnson@example.com",
            "city": "Springfield", 
            "state": "NY"
        },
        {
            "id": "patient-003",
            "family": "Davis",
            "given": ["Michael", "Robert"],
            "gender": "male",
            "birth_date": "1978-11-08",
            "phone": "555-9012",
            "email": "michael.davis@example.com",
            "city": "Riverside",
            "state": "TX"
        }
    ]
    
    for i in range(min(count, len(base_patients))):
        patient_data = base_patients[i]
        patient = {
            "resourceType": "Patient",
            "id": patient_data["id"],
            "active": True,
            "name": [{
                "family": patient_data["family"],
                "given": patient_data["given"]
            }],
            "gender": patient_data["gender"],
            "birthDate": patient_data["birth_date"],
            "telecom": [
                {
                    "system": "phone",
                    "value": patient_data["phone"],
                    "use": "home"
                },
                {
                    "system": "email", 
                    "value": patient_data["email"]
                }
            ],
            "address": [{
                "use": "home",
                "line": [f"{100 + i * 100} Main St"],
                "city": patient_data["city"],
                "state": patient_data["state"],
                "postalCode": f"{12345 + i}",
                "country": "US"
            }]
        }
        patients.append(patient)
    
    # Generate additional patients if requested
    for i in range(len(base_patients), count):
        patient = {
            "resourceType": "Patient",
            "id": f"patient-{i+1:03d}",
            "active": True,
            "name": [{
                "family": f"TestFamily{i+1}",
                "given": [f"TestGiven{i+1}"]
            }],
            "gender": "male" if i % 2 == 0 else "female",
            "birthDate": f"{1970 + (i % 50)}-{1 + (i % 12):02d}-{1 + (i % 28):02d}",
            "telecom": [{
                "system": "phone",
                "value": f"555-{1000 + i:04d}",
                "use": "home"
            }]
        }
        patients.append(patient)
    
    return patients


def generate_test_observations(patient_ids: List[str], count_per_patient: int = 2) -> List[Dict[str, Any]]:
    """
    Generate test Observation resources.
    
    Args:
        patient_ids: List of patient IDs to create observations for
        count_per_patient: Number of observations per patient
        
    Returns:
        List of Observation resource dictionaries
    """
    observations = []
    
    observation_types = [
        {
            "code": "85354-9",
            "display": "Blood pressure panel",
            "category": "vital-signs",
            "value": 120.0,
            "unit": "mmHg"
        },
        {
            "code": "4548-4", 
            "display": "Hemoglobin A1c",
            "category": "laboratory",
            "value": 6.8,
            "unit": "%"
        },
        {
            "code": "29463-7",
            "display": "Body Weight", 
            "category": "vital-signs",
            "value": 75.5,
            "unit": "kg"
        },
        {
            "code": "8480-6",
            "display": "Systolic blood pressure",
            "category": "vital-signs", 
            "value": 125.0,
            "unit": "mmHg"
        }
    ]
    
    obs_id = 1
    for patient_id in patient_ids:
        for i in range(count_per_patient):
            obs_type = observation_types[i % len(observation_types)]
            
            observation = {
                "resourceType": "Observation",
                "id": f"obs-{obs_id:03d}",
                "status": "final",
                "category": [{
                    "coding": [{
                        "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                        "code": obs_type["category"],
                        "display": obs_type["category"].replace("-", " ").title()
                    }]
                }],
                "code": {
                    "coding": [{
                        "system": "http://loinc.org",
                        "code": obs_type["code"],
                        "display": obs_type["display"]
                    }]
                },
                "subject": {"reference": f"Patient/{patient_id}"},
                "effectiveDateTime": (datetime.now() - timedelta(days=i*30)).isoformat() + "Z",
                "valueQuantity": {
                    "value": obs_type["value"] + (i * 0.5),  # Slight variation
                    "unit": obs_type["unit"],
                    "system": "http://unitsofmeasure.org",
                    "code": obs_type["unit"]
                }
            }
            observations.append(observation)
            obs_id += 1
    
    return observations


def generate_test_medications(patient_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Generate test MedicationRequest resources.
    
    Args:
        patient_ids: List of patient IDs to create medications for
        
    Returns:
        List of MedicationRequest resource dictionaries
    """
    medications = []
    
    medication_types = [
        {
            "code": "860975",
            "display": "Lisinopril 10 MG Oral Tablet",
            "dosage": "Take 1 tablet by mouth once daily"
        },
        {
            "code": "860856", 
            "display": "Metformin 500 MG Oral Tablet",
            "dosage": "Take 1 tablet by mouth twice daily with meals"
        },
        {
            "code": "197884",
            "display": "Atorvastatin 20 MG Oral Tablet", 
            "dosage": "Take 1 tablet by mouth once daily at bedtime"
        }
    ]
    
    med_id = 1
    for i, patient_id in enumerate(patient_ids):
        if i < len(medication_types):  # Not all patients get medications
            med_type = medication_types[i]
            
            medication = {
                "resourceType": "MedicationRequest",
                "id": f"med-{med_id:03d}",
                "status": "active",
                "intent": "order",
                "medicationCodeableConcept": {
                    "coding": [{
                        "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                        "code": med_type["code"],
                        "display": med_type["display"]
                    }]
                },
                "subject": {"reference": f"Patient/{patient_id}"},
                "authoredOn": (datetime.now() - timedelta(days=30)).isoformat() + "Z",
                "dosageInstruction": [{
                    "text": med_type["dosage"],
                    "route": {
                        "coding": [{
                            "system": "http://snomed.info/sct",
                            "code": "26643006",
                            "display": "Oral"
                        }]
                    }
                }]
            }
            medications.append(medication)
            med_id += 1
    
    return medications


def generate_test_encounters(patient_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Generate test Encounter resources.
    
    Args:
        patient_ids: List of patient IDs to create encounters for
        
    Returns:
        List of Encounter resource dictionaries
    """
    encounters = []
    
    encounter_types = [
        {
            "code": "185349003",
            "display": "Encounter for check up",
            "class_code": "AMB",
            "class_display": "ambulatory"
        },
        {
            "code": "185347001",
            "display": "Encounter for problem",
            "class_code": "AMB", 
            "class_display": "ambulatory"
        }
    ]
    
    enc_id = 1
    for i, patient_id in enumerate(patient_ids):
        enc_type = encounter_types[i % len(encounter_types)]
        start_time = datetime.now() - timedelta(days=i*15)
        end_time = start_time + timedelta(hours=1)
        
        encounter = {
            "resourceType": "Encounter",
            "id": f"enc-{enc_id:03d}",
            "status": "finished",
            "class": {
                "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                "code": enc_type["class_code"],
                "display": enc_type["class_display"]
            },
            "type": [{
                "coding": [{
                    "system": "http://snomed.info/sct",
                    "code": enc_type["code"],
                    "display": enc_type["display"]
                }]
            }],
            "subject": {"reference": f"Patient/{patient_id}"},
            "period": {
                "start": start_time.isoformat() + "Z",
                "end": end_time.isoformat() + "Z"
            },
            "serviceProvider": {"reference": "Organization/org-001"}
        }
        encounters.append(encounter)
        enc_id += 1
    
    return encounters


def generate_comprehensive_test_dataset(patient_count: int = 3) -> List[Dict[str, Any]]:
    """
    Generate a comprehensive test dataset with all resource types.
    
    Args:
        patient_count: Number of patients to generate
        
    Returns:
        List of all FHIR resources for testing
    """
    # Generate patients first
    patients = generate_test_patients(patient_count)
    patient_ids = [p["id"] for p in patients]
    
    # Generate related resources
    observations = generate_test_observations(patient_ids, count_per_patient=2)
    medications = generate_test_medications(patient_ids)
    encounters = generate_test_encounters(patient_ids)
    
    # Combine all resources
    all_resources = patients + observations + medications + encounters
    
    return all_resources


def save_test_dataset_to_file(file_path: str, patient_count: int = 3):
    """
    Generate and save test dataset to JSON file.
    
    Args:
        file_path: Path to save the JSON file
        patient_count: Number of patients to generate
    """
    dataset = generate_comprehensive_test_dataset(patient_count)
    
    with open(file_path, 'w') as f:
        json.dump(dataset, f, indent=2)
    
    return len(dataset)


# Convenience function for quick test data
def get_minimal_test_data() -> List[Dict[str, Any]]:
    """Get minimal test data for quick tests."""
    return generate_comprehensive_test_dataset(patient_count=2)


def get_standard_test_data() -> List[Dict[str, Any]]:
    """Get standard test data for comprehensive tests.""" 
    return generate_comprehensive_test_dataset(patient_count=3)


def get_large_test_data() -> List[Dict[str, Any]]:
    """Get larger test data for performance tests."""
    return generate_comprehensive_test_dataset(patient_count=10)