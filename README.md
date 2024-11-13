[![CI](https://github.com/nogibjj/Su_Zhang_PySpark_Project/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Su_Zhang_PySpark_Project/actions/workflows/cicd.yml)

# Su Zhang PySpark Project

This project demonstrates how to build an **ETL (Extract, Transform, Load)** pipeline using **PySpark** for large dataset processing. The pipeline extracts data from a CSV file, transforms it by applying certain calculations, and loads it into a DataFrame for analysis. The project also includes unit tests to ensure the correctness of each function.

## Project Overview

The goal of this project is to implement and test an ETL pipeline using PySpark. The ETL process is divided into three main steps:

1. **Extract**: Fetch data from a remote source or a local file.
2. **Transform**: Apply transformations to the data, such as calculating new columns or filtering data.
3. **Load**: Load the transformed data into a DataFrame and prepare it for further analysis or processing.

### Key Features
- **Data Extraction**: Fetch data from a remote source or a local CSV file.
- **Data Transformation**: Apply operations such as filtering, aggregating, and calculating new columns.
- **Data Loading**: Load data into a PySpark DataFrame for further analysis.
- **Testing**: Unit tests to validate the ETL pipeline components, using `pytest` and PySpark.

## Project Structure

```plaintext
PySpark_ETL_Project/
│
├── mylib/                    # Contains the core logic for ETL process
│   ├── calculator.py          # Contains ETL functions (extract, transform, load, etc.)
│   └── __init__.py
│
├── data/                     # Folder containing raw data files (e.g., drinks.csv)
│   └── drinks.csv
│
├── tests/                    # Unit tests for the ETL pipeline
│   └── test_main.py           # Test file for the ETL pipeline
│
├── README.md                 # Project description
├── requirements.txt          # List of dependencies
└── main.py                   # Main entry point for running the pipeline
```

## Requirements

Before you begin, ensure you have the following installed:

- **PySpark**: The main library for working with Spark in Python.
- **pytest**: For running the unit tests.

### Install Dependencies

You can install the required dependencies by running the following command in terminal:

```bash
make install
```

Alternatively, you can manually install the dependencies using:

```bash
pip install pyspark
```

## Setup Instructions

1. **Run on codespace** or **Clone the Repository**:

   * Open codespace and wait for the environment to set up

   * Clone this repository to your local machine.

   ```bash
   git clone https://github.com/nogibjj/Su_Zhang_PySpark_Project.git
   ```

2. **Prepare the Data**:

   Ensure that the `data/drinks.csv` file exists in the `data/` directory.


3. **Running the ETL Pipeline**:

   To run the ETL pipeline, use the following command:

   ```bash
   python main.py
   ```

   This will execute the ETL process, performing the following:
   - Extract the data from `data/drinks.csv`.
   - Apply the transformation (e.g., calculate `beer_percentage`).
   - Load the data into a PySpark DataFrame and display the results.

4. **Running Unit Tests**:

   You can run the unit tests to ensure that all functions are working as expected. To do so, use `pytest`:

   ```bash
   pytest test_main.py
   ```

   This will run all the test functions and provide a report of passed/failed tests.

## Logged Output

The logged markdown file could be found [here](https://github.com/nogibjj/Su_Zhang_PySpark_Project/blob/main/pyspark_process.md).

## References

* https://github.com/nogibjj/python-ruff-template


