# EVA_ETL
```markdown
# Data Processing Pipeline

This project implements a data processing pipeline using Prefect for ETL (Extract, Transform, Load) tasks. The pipeline reads data from a MySQL database, processes it to create pivot tables, and uploads the results to Excel files.

## Prerequisites

- Python 3.7+
- MySQL server
- Prefect
- SQLAlchemy
- Pandas

## Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/yourusername/data-processing-pipeline.git
   cd data-processing-pipeline
   ```

2. **Create and activate a virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate   # On Windows, use `venv\\Scripts\\activate`
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up the database**:
    - Ensure MySQL server is running.
    - Execute the SQL script to create and populate the database:
      ```bash
      mysql -u root -p < create_table.sql
      ```

## Configuration

Edit the `fields.json` file to specify the tables and fields for generating SQL queries.

## Usage

Run the data processing flow:
```bash
python main.py
```

The script performs the following steps:

1. Reads the last run time from `last_run_time.txt`.
2. Loads configurations from `fields.json`.
3. Connects to the MySQL database.
4. Generates and executes SQL queries based on the configurations.
5. Creates pivot tables from the query results.
6. Uploads the results to Excel files.
7. Updates the last run time.


## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## Acknowledgements

- Prefect
- SQLAlchemy
- Pandas
```
