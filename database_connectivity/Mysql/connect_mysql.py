import mysql.connector
from mysql.connector import Error

def connect_mysql_and_execute(query: str):
    try:
        # Establish a connection to the MySQL database
        connection = mysql.connector.connect(
            host='<host_name>',
            user='<username>',
            password='<password>',
            database='<db_name>'
        )

        if connection.is_connected():
            print("Connected to MySQL database.")

            # Create a cursor and execute the query
            cursor = connection.cursor()
            cursor.execute(query)

            # Fetch all rows
            rows = cursor.fetchall()

            # Get column names
            column_names = [i[0] for i in cursor.description]

            # Print each row
            for row in rows:
                entry = dict(zip(column_names, row))
            print(rows.__len__())

    except Error as e:
        print(f"Error: {e}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            print("MySQL connection closed.")

# Run the function
if __name__ == "__main__":
    sql_query = 'select * from <db_name>.<table_name>'
    connect_mysql_and_execute(query = sql_query)
