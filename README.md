# ARX-Presidio Data Anonymization Pipeline

This project implements a full pipeline for pseudonymization and anonymization of relational databases, ensuring referential integrity is preserved. It combines the [ARX](https://arx.deidentifier.org/) anonymization framework with Microsoft’s [Presidio](https://microsoft.github.io/presidio/) for automatic attribute classification. The pipeline is tailored for health-related data but can be adapted to other domains.

## Features

- Automatic connection to MySQL databases
- Detection of primary and foreign keys
- Attribute classification using Presidio (via REST API)
- Consistent pseudonymization of identifiers (PKs and FKs)
- Referential integrity preservation across all tables
- Definition and application of custom generalization hierarchies
- Application of ARX privacy models (k-anonymity, l-diversity)
- Optional export/import of generalization hierarchies
- Output: anonymized and pseudonymized tables ready for analysis or sharing

## Pipeline Overview

1. **Initialize Presidio** through a local REST API
2. **Extract metadata** (PKs and FKs) from the database
3. **Classify each column** using Presidio (with data sampling)
4. **Pseudonymize identifiers**, preserving relational links
5. **Ensure referential integrity** throughout all tables
6. **Define generalization hierarchies** (e.g., for race, gender, blood type)
7. **Apply ARX anonymization**, using selected privacy models
8. **Export results** back to the database as new tables

A detailed flowchart is included in the repository to illustrate the full process.

## Project Structure

1. **ARXUtils.java # Utility class for ARX anonymization and data loading
1. **PresidioPIIClassifier.java # Attribute classification via Presidio Analyzer
1. **Pseudonymizer.java # Consistent pseudonym generation
1. **DatabaseMetadataUtils.java # PK/FK extraction from DB metadata
1. **CustomRaceHierarchyBuilder.java # Custom hierarchy for race
1. **CustomGenderHierarchyBuilder.java # Custom hierarchy for gender
1. **CustomBloodTypeHierarchyBuilder.java# Custom hierarchy for blood type
1. **Main.java # Main orchestrator class


## Requirements

- Java 8 or higher
- Maven
- ARX Library (as dependency)
- Python 3.7+ for Presidio Analyzer
- Internet access **not required** (Presidio runs locally)

## Python Setup (Presidio)

Presidio must be running locally as a REST service. To install and launch:

```bash
pip install presidio-analyzer
uvicorn presidio_analyzer_entry:app --host 127.0.0.1 --port 3000


