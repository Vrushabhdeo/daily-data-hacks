import psycopg2

def get_redshift_connection(schema_name):
    # Redshift connection details
    conn = psycopg2.connect(
        host='<host_name>',
        port='5439',
        database={schema_name},
        user='<username>',
        password='<password>'
    )
    return conn, conn.cursor()

if __name__ == '__main__':

    schema_name = '<schema_name>'

    # Get Redshift Connection
    conn, cur = get_redshift_connection(schema_name=schema_name)

    # Step 1: Get the list of tables in the schema
    cur.execute(f"""
        SELECT table_name
        FROM SVV_TABLES
        WHERE table_schema = {schema_name}'
          AND table_type = 'BASE TABLE';
    """)

    tables = cur.fetchall()

    # Step 2: For each table, get the count
    for table in tables:
        table_name = table
        query = f"SELECT COUNT(*) FROM prod.{schema_name}.{table_name}"

        try:
            cur.execute(query)
            count = cur.fetchone()[0]
            print(f"{table_name},{count}")
        except Exception as e:
            print(f"Error querying {table_name}: {e}")

    # Close the connection
    cur.close()
    conn.close()
