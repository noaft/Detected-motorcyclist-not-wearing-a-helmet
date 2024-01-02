import psycopg2
def connect_postgresql():
    conn_params = {
        'database': 'customers',
        'user': 'postgres',
        'password': '121203Toan',
        'host': 'localhost',
        'port': 5432
    }
    
    return psycopg2.connect(**conn_params)

def save_data_to_postgresql(frame, date, track_id):

    # Connect to PostgreSQL using a context manager
    with connect_postgresql() as conn:
        # Create a cursor
        with conn.cursor() as cursor:
            # SQL query with placeholders
            query = "INSERT INTO images (track_id, img_, date_) VALUES(%s, %s, %s)"
            # Execute the query with actual values
            cursor.execute(query, (10,100002,'23-0-2003'))
        # Commit the changes to the database
        conn.commit()
save_data_to_postgresql(frame, date, track_id)
