import json
from sqlalchemy import create_engine
import pandas as pd
from prefect import flow, task
from datetime import datetime, timedelta
import os

@task(log_prints=True)
def save_last_run_time(file_path="last_run_time.txt"):
    current_time = datetime.now()
    flow_run_ctx = current_time - timedelta(hours=12)
    with open(file_path, "w") as file:
        file.write(flow_run_ctx.strftime("%Y-%m-%dT%H:%M:%S"))

@task(log_prints=True)
def get_last_run_time(file_path="last_run_time.txt"):
    if not os.path.exists(file_path):
        # Initialize the file with a default time if it doesn't exist
        flow_run_ctx = datetime.now() - timedelta(hours=12)
        with open(file_path, "w") as file:
            file.write(flow_run_ctx.strftime("%Y-%m-%dT%H:%M:%S"))
        return flow_run_ctx.strftime("%Y-%m-%dT%H:%M:%S")

    with open(file_path, "r") as file:
        last_run_time = file.read().strip()
    return last_run_time

@task(log_prints=True)
def generate_sql_query(table_name, values, field_name, datetime_field, last_flow_run):
    """
    Generates a SQL query to retrieve data from a table based on specified values and field.

    Args:
        table_name (str): The name of the table to query.
        values (list): A list of values to filter by.
        field_name (str): The name of the field to filter on.
        datetime_field (str): The datetime field to filter by the last run time.
        last_flow_run (str): The last flow run time.

    Returns:
        str: A SQL query string.
    """
    if field_name == 'name':
        last_flow_run_iso = datetime.strptime(last_flow_run, "%Y-%m-%dT%H:%M:%S")
        last_flow_run_formatted = last_flow_run_iso.strftime("%Y-%m-%d %H:%M:%S")
        values_str = ', '.join([f'"{value}"' for value in values])
        return f"SELECT * FROM {table_name} WHERE attribute_name IN ({values_str}) AND {datetime_field} > '{last_flow_run_formatted}';"
    elif field_name == 'original_field':
        return f"SELECT {', '.join(values)} FROM {table_name}"

@task(log_prints=True)
def execute_query(query, engine):
    """
    Executes a SQL query and returns the result as a DataFrame.

    Args:
        query (str): The SQL query to execute.
        engine (Engine): The SQLAlchemy engine to use for the query.

    Returns:
        DataFrame: The query result as a pandas DataFrame.
    """
    return pd.read_sql(query, engine)

@task(log_prints=True)
def create_pivot_table(df, pivot_index, pivot_columns, pivot_values, other_fields):
    """
    Creates a pivot table from a DataFrame.

    Args:
        df (DataFrame): The DataFrame to pivot.
        pivot_index (str): The index to pivot on.
        pivot_columns (str): The columns to pivot.
        pivot_values (str): The values to aggregate.
        other_fields (list): List of other fields to include in the result.

    Returns:
        DataFrame: The resulting pivoted DataFrame.
    """
    pivot_df = df.pivot_table(index=pivot_index, columns=pivot_columns, values=pivot_values, aggfunc='first')
    if pivot_df.index.name is None:
        pivot_df.index.name = pivot_index  # Ensure index name is set for merge
    result_df = pd.merge(df[other_fields].drop_duplicates(), pivot_df, on=pivot_index)
    return result_df

@task(log_prints=True)
def upload_data(df, table_name):
    """
    Uploads data to an Excel file (placeholder for DB upload).

    Args:
        df (DataFrame): The DataFrame to upload.
        table_name (str): The name of the table to upload data to (currently used as a filename).

    Returns:
        None
    """
    df.to_excel(f"{table_name}.xlsx")
    # Placeholder: Implement actual DB upload logic here

@flow(name="eav_model_transform")
def run_flow():
    """
    Main flow to read configurations, generate queries, execute them, and process results.

    Returns:
        None
    """
    last_flow_run = get_last_run_time()
    with open('fields.json') as f:
        fields_dict = json.load(f)

    db_uri = 'mysql://root:root@localhost/medical_db'
    engine = create_engine(db_uri)

    for entry in fields_dict:
        table_name = entry['table_name']
        values = entry['values']
        field_name = entry['field_name']
        datetime_field = entry.get('datetime_field', 'timestamp')
        sql_query = generate_sql_query(table_name, values, field_name, datetime_field, last_flow_run)
        print(sql_query)

        pivot_index = entry.get("pivot_index")
        pivot_columns = entry.get("pivot_columns")
        pivot_values = entry.get("pivot_values")
        other_fields = entry.get("other_fields")

        query_result = execute_query(sql_query, engine)

        if pivot_index is not None:
            pivot_table_result = create_pivot_table(query_result, pivot_index, pivot_columns, pivot_values, other_fields)
            upload_data(pivot_table_result, table_name)
        else:
            upload_data(query_result, table_name)

    save_last_run_time()

if __name__ == "__main__":
    run_flow()
