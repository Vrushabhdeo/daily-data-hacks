import trino
import urllib3

# Suppress only InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_trino_connection(host, port, catalog, schema, username, password):
    try:
        return trino.dbapi.connect(
            host=host,
            port=port,
            catalog=catalog,
            schema=schema,
            verify=False,
            http_scheme="https",
            auth=trino.auth.BasicAuthentication(username, password))
    except Exception as error:
        raise RuntimeError(f"Error occurred while connecting to Trino..\n {error}")


def execute_trino_query(trino_conn, sql_query):
    try:
        cursor = trino_conn.cursor()
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        return rows
    except trino.exceptions.TrinoQueryError as e:
        print(f"Trino query failed to execute: {e}")
    except Exception as e:
        print(f"An exception occurred while executing the query: {e}")
    finally:
        cursor.close()
        trino_conn.close()


def get_table_counts(trino_conn, db_name, table_name):
    """
    Fetch row counts for a given table and its metadata table.

    :param trino_conn: Active Trino connection object.
    :param db_name: Database (schema) name.
    :param table_name: Table name.
    """

    try:
        # Define queries
        main_table_query = f"SELECT COUNT(*) FROM {db_name}.{table_name}"

        # Execute queries
        main_table_count = execute_trino_query(trino_conn, main_table_query, trino_conn_id=db_name)

        # Print results
        print(f"Base Table count {db_name}.{table_name}: {main_table_count[0][0]}")

    except Exception as e:
        print("Something went wrong.....")
        print(e)


# Example usage:
if __name__ == "__main__":
    # host = 'https://dp-emr-prod-spark-ascent-docon-gcp.prod.dataplatform.link'
    host = '<host>'
    port = 8442
    username = '<username>'
    password = '<password>'
    catalog = 'hive'
    schema = '<db_name>'

    # Connect to Trino
    trino_conn = get_trino_connection(host, port, catalog, schema, username, password)

    # List of tables
    table_name = ['<table_name1>', '<table_name2>', '<table_name3>', '<table_name4>']

    try:
        for db_hive_table_name in table_name:
            # Fetch and print row counts
            get_table_counts(trino_conn, schema, db_hive_table_name)
            print("#####"*5)
    except Exception as e:
        print("Something went wrong.....")
        print(e)