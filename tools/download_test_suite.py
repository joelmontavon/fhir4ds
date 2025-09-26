import json
import pathlib
import urllib.request

# The base URL for the raw test files on GitHub
BASE_URL = "https://raw.githubusercontent.com/HL7/fhirpath/master/tests"

# A list of known test file names. This can be expanded later.
TEST_FILES = [
    "test-patient.json",
    "test-element-definition.json",
    "test-observation.json",
    "test-questionnaire.json",
    "test-spec.json",
]

def download_test_suite(output_dir: pathlib.Path):
    """
    Downloads the official FHIRPath R4 test suite from GitHub.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    for file_name in TEST_FILES:
        url = f"{BASE_URL}/{file_name}"
        output_path = output_dir / file_name

        try:
            print(f"Downloading {url} to {output_path}...")
            urllib.request.urlretrieve(url, output_path)
        except urllib.error.HTTPError as e:
            print(f"Error downloading {url}: {e}")

if __name__ == "__main__":
    # The directory where the test files will be saved
    test_suite_dir = pathlib.Path("tests/official/fhirpath_r4")

    download_test_suite(test_suite_dir)

    print("\nTest suite download complete.")