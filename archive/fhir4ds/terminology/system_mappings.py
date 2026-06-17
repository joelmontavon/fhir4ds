"""
FHIR Terminology System Mappings

Comprehensive mapping between different terminology system identifiers:
- OID (Object Identifier): numeric like 2.16.840.1.113883.6.96
- URI (Uniform Resource Identifier): like http://snomed.info/sct
- URN (Uniform Resource Name): like urn:oid:2.16.840.1.113883.6.96

Based on FHIR R4 specification and common healthcare terminology systems.
"""

from typing import List, Dict, Any

# Comprehensive terminology system mappings
# Each entry contains: (canonical_uri, oid, urn_oid, name, description)
TERMINOLOGY_SYSTEM_MAPPINGS: List[Dict[str, Any]] = [
    # SNOMED CT
    {
        'canonical_uri': 'http://snomed.info/sct',
        'oid': '2.16.840.1.113883.6.96',
        'urn_oid': 'urn:oid:2.16.840.1.113883.6.96',
        'name': 'SNOMED CT',
        'description': 'Systematized Nomenclature of Medicine Clinical Terms'
    },

    # LOINC
    {
        'canonical_uri': 'http://loinc.org',
        'oid': '2.16.840.1.113883.6.1',
        'urn_oid': 'urn:oid:2.16.840.1.113883.6.1',
        'name': 'LOINC',
        'description': 'Logical Observation Identifiers Names and Codes'
    },

    # ICD-10-CM
    {
        'canonical_uri': 'http://hl7.org/fhir/sid/icd-10-cm',
        'oid': '2.16.840.1.113883.6.90',
        'urn_oid': 'urn:oid:2.16.840.1.113883.6.90',
        'name': 'ICD-10-CM',
        'description': 'International Classification of Diseases, 10th Revision, Clinical Modification'
    },

    # ICD-10-PCS
    {
        'canonical_uri': 'http://hl7.org/fhir/sid/icd-10-pcs',
        'oid': '2.16.840.1.113883.6.4',
        'urn_oid': 'urn:oid:2.16.840.1.113883.6.4',
        'name': 'ICD-10-PCS',
        'description': 'International Classification of Diseases, 10th Revision, Procedure Coding System'
    },

    # RxNorm
    {
        'canonical_uri': 'http://www.nlm.nih.gov/research/umls/rxnorm',
        'oid': '2.16.840.1.113883.6.88',
        'urn_oid': 'urn:oid:2.16.840.1.113883.6.88',
        'name': 'RxNorm',
        'description': 'RxNorm medication terminology'
    },

    # CPT
    {
        'canonical_uri': 'http://www.ama-assn.org/go/cpt',
        'oid': '2.16.840.1.113883.6.12',
        'urn_oid': 'urn:oid:2.16.840.1.113883.6.12',
        'name': 'CPT',
        'description': 'Current Procedural Terminology'
    },

    # HCPCS
    {
        'canonical_uri': 'http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets',
        'oid': '2.16.840.1.113883.6.285',
        'urn_oid': 'urn:oid:2.16.840.1.113883.6.285',
        'name': 'HCPCS',
        'description': 'Healthcare Common Procedure Coding System'
    },

    # NDC
    {
        'canonical_uri': 'http://hl7.org/fhir/sid/ndc',
        'oid': '2.16.840.1.113883.6.69',
        'urn_oid': 'urn:oid:2.16.840.1.113883.6.69',
        'name': 'NDC',
        'description': 'National Drug Code'
    },

    # CVX (Vaccine Codes)
    {
        'canonical_uri': 'http://hl7.org/fhir/sid/cvx',
        'oid': '2.16.840.1.113883.12.292',
        'urn_oid': 'urn:oid:2.16.840.1.113883.12.292',
        'name': 'CVX',
        'description': 'Vaccine Administered (CVX)'
    },

    # ICD-9-CM
    {
        'canonical_uri': 'http://hl7.org/fhir/sid/icd-9-cm',
        'oid': '2.16.840.1.113883.6.103',
        'urn_oid': 'urn:oid:2.16.840.1.113883.6.103',
        'name': 'ICD-9-CM',
        'description': 'International Classification of Diseases, 9th Revision, Clinical Modification'
    },

    # UCUM
    {
        'canonical_uri': 'http://unitsofmeasure.org',
        'oid': '2.16.840.1.113883.6.8',
        'urn_oid': 'urn:oid:2.16.840.1.113883.6.8',
        'name': 'UCUM',
        'description': 'Unified Code for Units of Measure'
    },

    # HL7 v2 Tables
    {
        'canonical_uri': 'http://terminology.hl7.org/CodeSystem/v2-0203',
        'oid': '2.16.840.1.113883.18.108',
        'urn_oid': 'urn:oid:2.16.840.1.113883.18.108',
        'name': 'HL7 v2 Identifier Type',
        'description': 'HL7 Version 2 Identifier Type codes'
    },

    # NUBC Revenue Codes
    {
        'canonical_uri': 'http://www.nubc.org/revenue-codes',
        'oid': '2.16.840.1.113883.6.13',
        'urn_oid': 'urn:oid:2.16.840.1.113883.6.13',
        'name': 'NUBC Revenue Codes',
        'description': 'National Uniform Billing Committee Revenue Codes'
    },

    # NUBC Point of Origin
    {
        'canonical_uri': 'http://www.nubc.org/point-of-origin',
        'oid': '2.16.840.1.113883.6.301',
        'urn_oid': 'urn:oid:2.16.840.1.113883.6.301',
        'name': 'NUBC Point of Origin',
        'description': 'National Uniform Billing Committee Point of Origin codes'
    },

    # NUBC Priority Type
    {
        'canonical_uri': 'http://www.nubc.org/priority-type',
        'oid': '2.16.840.1.113883.6.302',
        'urn_oid': 'urn:oid:2.16.840.1.113883.6.302',
        'name': 'NUBC Priority Type',
        'description': 'National Uniform Billing Committee Priority Type codes'
    },

    # NUBC Discharge Status
    {
        'canonical_uri': 'http://www.nubc.org/patient-discharge',
        'oid': '2.16.840.1.113883.6.18',
        'urn_oid': 'urn:oid:2.16.840.1.113883.6.18',
        'name': 'NUBC Discharge Status',
        'description': 'National Uniform Billing Committee Patient Discharge Status codes'
    },

    # Race and Ethnicity CDC
    {
        'canonical_uri': 'urn:oid:2.16.840.1.113883.6.238',
        'oid': '2.16.840.1.113883.6.238',
        'urn_oid': None,  # Same as canonical_uri, so no separate URN mapping needed
        'name': 'Race and Ethnicity CDC',
        'description': 'CDC Race and Ethnicity codes'
    },

    # NCPDP
    {
        'canonical_uri': 'http://terminology.hl7.org/CodeSystem/NCPDPDispensedAsWrittenOrProductSelectionCode',
        'oid': '2.16.840.1.113883.6.345',
        'urn_oid': 'urn:oid:2.16.840.1.113883.6.345',
        'name': 'NCPDP',
        'description': 'National Council for Prescription Drug Programs'
    },

    # Medicare Provider
    {
        'canonical_uri': 'http://terminology.hl7.org/CodeSystem/medicare-provider-type',
        'oid': '2.16.840.1.113883.6.335',
        'urn_oid': 'urn:oid:2.16.840.1.113883.6.335',
        'name': 'Medicare Provider Type',
        'description': 'Medicare Provider Type codes'
    }
]


def get_all_system_identifiers() -> List[Dict[str, str]]:
    """
    Get all system identifier mappings for terminology system crosswalking.

    Returns list of dicts with keys: original_system, canonical_system, system_type
    This creates all possible mappings: OID->URI, URN->URI, URI->URI
    """
    mappings = []

    for system in TERMINOLOGY_SYSTEM_MAPPINGS:
        canonical = system['canonical_uri']

        # URI -> URI (identity mapping)
        mappings.append({
            'original_system': canonical,
            'canonical_system': canonical,
            'system_type': 'uri',
            'name': system['name']
        })

        # OID -> URI mapping (if OID exists)
        if system.get('oid'):
            mappings.append({
                'original_system': system['oid'],
                'canonical_system': canonical,
                'system_type': 'oid',
                'name': system['name']
            })

        # URN:OID -> URI mapping (if URN exists)
        if system.get('urn_oid'):
            mappings.append({
                'original_system': system['urn_oid'],
                'canonical_system': canonical,
                'system_type': 'urn_oid',
                'name': system['name']
            })

    return mappings


def get_canonical_system(original_system: str) -> str:
    """
    Get canonical URI for a given terminology system identifier.

    Args:
        original_system: Original system identifier (OID, URI, or URN)

    Returns:
        Canonical URI, or original_system if no mapping found
    """
    for mapping in get_all_system_identifiers():
        if mapping['original_system'] == original_system:
            return mapping['canonical_system']

    # Fallback: return original system
    return original_system