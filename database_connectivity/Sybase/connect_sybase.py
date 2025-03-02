import jaydebeapi
import sys
from contextlib import closing

import jpype
import os

def get_conn():

    """Return the Sybase connection"""

    jconn_path = 'jconn4.jar'
    url = 'jdbc:sybase:Tds:{host}:{port}?ServiceName={db}'.format(host='<host_name>',
                                                                          port=str(2638),
                                                                          db='<db_name>')
    # Ensure Java classpath is set
    os.environ['CLASSPATH'] = 'jconn4.jar'

    # Start JVM manually
    if not jpype.isJVMStarted():
        jpype.startJVM(classpath=['jconn4.jar'])

    # Load the driver explicitly
    SybDriver = jpype.JClass('com.sybase.jdbc4.jdbc.SybDriver')

    conn = jaydebeapi.connect(jclassname=SybDriver,
                              url=url,
                              driver_args=['<username>', '<password>'],
                              jars=[jconn_path])
    return conn

if __name__ == '__main__':

    sql = 'select * from <db_name>.<table_name>'

    # as_dict Set to True in case dictionary results
    as_dict = True

    if sys.version_info[0] < 3:
        sql = sql.encode('utf-8')

    with closing(get_conn()) as conn:
        with closing(conn.cursor()) as cur:
                cur.execute(sql)
                results = cur.fetchall()

                if as_dict:
                    # Fetch column names
                    columns = [desc[0] for desc in cur.description]
                    result = [dict(zip(columns, row)) for row in results]
    print(result)