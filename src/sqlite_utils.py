import sqlite3

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
 
    return conn, conn.cursor()

def execute_query(q, db_file):
    conn, cur = create_connection(db_file)

    cur.execute(q)

    rows = [row for row in cur.fetchall()]

    cur.close()
    conn.close()

    return rows

def print_query(q, db_file='/mnt/snap/AdditionalFiles/track_metadata.db'):
    print(execute_query(q, db_file))




