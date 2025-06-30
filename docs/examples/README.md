# 📚 FHIR4DS Examples

**Comprehensive examples and demonstrations for FHIR4DS**

This directory contains interactive Jupyter notebooks, sample ViewDefinitions, and complete demonstrations of FHIR4DS functionality.

## 📁 Directory Structure

```
examples/
├── notebooks/                          # Interactive Jupyter demonstrations
│   ├── FHIR4DS_Quick_Start.ipynb      # 🚀 Perfect starting point for new users
│   ├── FHIR4DS_Complete_Feature_Demo.ipynb  # 🏥 Comprehensive feature showcase
│   ├── FHIR4DS_Server_Demo.ipynb      # 🌐 RESTful API server demonstration
│   └── FHIR4DS_Dual_Dialect_Testing.ipynb  # 🔄 Database compatibility testing
│
├── view_definitions/                    # Sample ViewDefinitions for common use cases
│   ├── simple_patient_fields.json     # Basic patient demographics
│   ├── patient_with_where.json        # Filtered patient queries
│   ├── omop_*.json                    # OMOP CDM mapping examples
│   └── perf_*.json                    # Performance testing ViewDefinitions
│
└── README.md                           # This file
```

## 🚀 Getting Started

### For New Users
**Start here**: [`FHIR4DS_Quick_Start.ipynb`](notebooks/FHIR4DS_Quick_Start.ipynb)
- One-line database setup
- Loading FHIR resources
- Creating ViewDefinitions
- Multi-format data export
- Performance optimization

### For Comprehensive Overview
**Complete demo**: [`FHIR4DS_Complete_Feature_Demo.ipynb`](notebooks/FHIR4DS_Complete_Feature_Demo.ipynb)
- All FHIR4DS features
- Real-world healthcare scenarios
- Performance benchmarking
- Advanced analytics patterns

### For API Development
**Server demo**: [`FHIR4DS_Server_Demo.ipynb`](notebooks/FHIR4DS_Server_Demo.ipynb)
- RESTful API endpoints
- ViewDefinition management
- Real-time analytics execution
- Bulk resource processing

### For Database Testing
**Dialect testing**: [`FHIR4DS_Dual_Dialect_Testing.ipynb`](notebooks/FHIR4DS_Dual_Dialect_Testing.ipynb)
- DuckDB vs PostgreSQL comparison
- SQL generation analysis
- Performance benchmarking
- 100% compliance validation

## 📊 Sample ViewDefinitions

### Basic Examples
- **`simple_patient_fields.json`** - Extract basic patient demographics
- **`patient_with_where.json`** - Filtered patient queries with conditions

### OMOP CDM Integration
- **`omop_person.json`** - Map FHIR Patient to OMOP Person table
- **`omop_observation.json`** - Map FHIR Observation to OMOP measurements
- **`omop_condition_occurrence.json`** - Map FHIR Condition to OMOP conditions

### Performance Testing
- **`perf_observation_choice_types.json`** - Test choice type handling
- **`perf_complex_references.json`** - Complex reference resolution
- **`perf_high_volume_claims.json`** - Large dataset processing

## 🎯 Usage Examples

### Load a Sample ViewDefinition
```python
import json
from fhir4ds.helpers import QuickConnect

# Load ViewDefinition
with open('examples/view_definitions/simple_patient_fields.json') as f:
    view_def = json.load(f)

# Setup database and execute
db = QuickConnect.duckdb("./demo.db")
results = db.execute_to_dataframe(view_def)
```

### Run Interactive Notebooks
```bash
# Start Jupyter
jupyter notebook examples/notebooks/

# Or use Jupyter Lab
jupyter lab examples/notebooks/
```

### Execute with Sample Data
```python
# Using sample FHIR data from datasets/
from pathlib import Path
import json

# Load sample data
sample_files = Path("datasets/coherent/").glob("*.json")
resources = []
for file in list(sample_files)[:5]:  # Load first 5 files
    with open(file) as f:
        bundle = json.load(f)
        if 'entry' in bundle:
            resources.extend([entry['resource'] for entry in bundle['entry']])

# Execute analytics
db.load_resources(resources)
results = db.execute_to_dataframe(view_def)
```

## 📈 Performance Characteristics

### Notebook Execution Times
- **Quick Start**: ~2-3 minutes (basic features)
- **Complete Demo**: ~5-10 minutes (comprehensive features)
- **Server Demo**: ~3-5 minutes (API demonstrations)
- **Dialect Testing**: ~5-15 minutes (depends on test count)

### Sample Dataset Processing
- **Small datasets** (10-100 resources): Instant execution
- **Medium datasets** (1K-10K resources): 1-5 seconds
- **Large datasets** (100K+ resources): 10-60 seconds

## 🛠️ Development Usage

### Creating New Examples
1. **Copy an existing notebook** as a template
2. **Modify the ViewDefinitions** for your use case
3. **Add sample data** relevant to your scenario
4. **Document the healthcare context** in markdown cells

### Testing ViewDefinitions
```python
# Validate ViewDefinition before using
from fhir4ds.view_runner import SQLOnFHIRViewRunner

runner = SQLOnFHIRViewRunner()
try:
    sql = runner.generate_sql(view_definition)
    print("✅ ViewDefinition is valid")
    print(f"Generated SQL: {sql}")
except Exception as e:
    print(f"❌ ViewDefinition error: {e}")
```

### Performance Testing
```python
import time

# Measure execution time
start_time = time.time()
results = db.execute_to_dataframe(view_definition)
execution_time = time.time() - start_time

print(f"Processed {len(results)} records in {execution_time:.3f}s")
print(f"Throughput: {len(results)/execution_time:.0f} records/sec")
```

## 🏥 Healthcare Use Cases

### Clinical Analytics
- **Patient cohort identification**
- **Quality measure calculations**
- **Population health analytics**
- **Clinical decision support**

### Research Applications
- **Phenotype definitions**
- **Outcome measurements**
- **Biomarker analysis**
- **Clinical trial recruitment**

### Operational Analytics
- **Resource utilization**
- **Care pathway analysis**
- **Performance metrics**
- **Financial analytics**

## 🔧 Troubleshooting

### Common Issues

1. **Jupyter Kernel Not Found**
   ```bash
   python -m ipykernel install --user --name fhir4ds
   ```

2. **Missing Dependencies**
   ```bash
   pip install jupyter pandas matplotlib seaborn
   ```

3. **ViewDefinition Errors**
   - Check JSON syntax
   - Validate FHIRPath expressions
   - Ensure resource types match data

4. **Performance Issues**
   - Use smaller datasets for initial testing
   - Enable parallel processing
   - Consider DuckDB for analytics workloads

### Getting Help
- **Check the notebook outputs** for detailed error messages
- **Review the API documentation** in `docs/API.md`
- **Examine working examples** for reference patterns
- **Use the Quick Start notebook** to verify basic functionality

## 🎉 Contributing

### Adding New Examples
1. Create descriptive notebooks with clear use cases
2. Include real-world healthcare scenarios
3. Provide sample data and expected outputs
4. Document performance characteristics
5. Test with both DuckDB and PostgreSQL

### Improving Existing Examples
- Add more detailed explanations
- Include additional healthcare context
- Optimize for better performance
- Expand error handling
- Add visualization examples

---

**Start with the Quick Start notebook and explore the power of FHIR analytics!** 🚀