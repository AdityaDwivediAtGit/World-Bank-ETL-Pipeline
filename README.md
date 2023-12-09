# World-Bank-ETL-Pipeline 

This repository contains the code for an ETL (Extract, Transform, Load) pipeline focused on World Bank data. The project involves cleaning and combining datasets from various sources, including CSV, JSON, and XML files. The goal is to create a unified dataset for predicting World Bank Project total costs using a machine learning model.

## Usage

### Prerequisites

- Ensure you have Python installed (version 3.6 or later).
- Install the required packages using:

  ```bash
  pip install -r requirements.txt
  ```

### Running the ETL Pipeline

1. Clone the repository:

    ```bash
    gh repo clone AdityaDwivediAtGit/World-Bank-ETL-Pipeline
    ```

2. Navigate to the project directory, install Prerequisites and unzip files:

    ```bash
    cd World-Bank-ETL-Pipeline
    pip install -r requirements.txt
    unzip archive_etl.zip
    ```

3. Run the ETL pipeline:

    ```bash
    python main.py
    ```

### Repository Structure

- **cleaned_files/**: Contains the cleaned CSV files. (This directory automatically generated after running main.py)
- **population_data.db, projects_data.csv, ...**: These are raw files that appears after you unzip `archive_etl.zip` and are present in same folder as `main.py`.

### Notes

- The ETL pipeline uses PySpark for efficient data processing. Ensure you have Java installed on your machine.
- Modify the `debug` variable in the `main.py` file to toggle debugging information.

### Contributions

You can read more about behind the scenes in `Documentation` directory.
Feel free to provide feedback or suggestions! Contributions are welcome.

---

**Author: Aditya Dwivedi**
```