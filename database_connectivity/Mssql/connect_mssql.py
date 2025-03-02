import pandas as pd
import pymssql

EY_TABLE_LIST = ["<table_name1>", "<table_name2>", "<table_name3>", "<table_name4>"]

def read_db_config(csv_path: str) -> pd.DataFrame:
    """
    Reads a CSV file containing database connection details and returns a pandas DataFrame.

    :param csv_path: Path to the CSV file.
    :return: Pandas DataFrame with database connection details.
    """
    return pd.read_csv(csv_path)

def get_mssql_connection_pymssql(host: str, port: int, username: str, password: str, database: str):
    """
    Establishes a connection to an MSSQL database using pymssql.

    :param host: Database host.
    :param port: Database port.
    :param username: Database username.
    :param password: Database password.
    :param database: Database name.
    :return: pymssql Connection object.
    """
    return pymssql.connect(
        server=host,
        port=port,
        user=username,
        password=password,
        database=database,
        as_dict=True,
        login_timeout=10,
        timeout=10
    )

def get_table_schema(database_name: str, table_name: str, conn) -> pd.DataFrame:
    """
    Fetches the schema of a given table from an MSSQL database.

    :param database_name: Name of the database.
    :param table_name: Name of the table.
    :param conn: Active database connection.
    :return: Pandas DataFrame containing column names and data types.
    """
    query = f"""
        SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
    """
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()

    if not results:
        print(f"No schema found for table: {table_name}")
        return pd.DataFrame()

    # Convert results into a DataFrame
    schema_df = pd.DataFrame(results, columns=["column_name", "data_type"])
    return schema_df

if __name__ == '__main__':

    # Read multiple database from csv
    csv_path = 'path/connections_details.csv'
    db_config = read_db_config(csv_path)

    # Filter for the row where schema is '<base_schema_name>'
    myp_slave_row = db_config[db_config["schema"] == "<base_schema_name>"]

    if not myp_slave_row.empty:
        base_host = myp_slave_row.iloc[0]["Host"]
        base_port = int(myp_slave_row.iloc[0]["port"])
        base_username = myp_slave_row.iloc[0]["login"]
        base_password = myp_slave_row.iloc[0]["password"]
        base_database = myp_slave_row.iloc[0]["schema"]

        # Establish connection
        base_connection = get_mssql_connection_pymssql(base_host, base_port, base_username, base_password,
                                                       base_database)

    for table in EY_TABLE_LIST:
        print('######'*10)
        print(f'Processing table: {table}')

        # Fetch table schema
        base_schema_df = get_table_schema(base_database, table, base_connection)
        print(base_schema_df)

        print('######' * 10)