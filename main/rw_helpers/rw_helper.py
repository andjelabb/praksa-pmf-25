import psycopg2
from psycopg2 import sql


RISINGWAVE_DB = "dev"
RISINGWAVE_HOST = "localhost"
RISINGWAVE_USER = "root"
RISINGWAVE_PASSWORD = "root"
RISINGWAVE_PORT = 4566

def get_connection():
    
    connection_params = {
        "host": RISINGWAVE_HOST,
        "port": RISINGWAVE_PORT,
        "user": RISINGWAVE_USER,
        "password": RISINGWAVE_PASSWORD,
        "database": RISINGWAVE_DB
    }

    try:
        conn = psycopg2.connect(**connection_params)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def execute_query(conn, query):
    
    try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                
                is_select = query.strip().upper().startswith('SELECT')
                
                if is_select:
                    result = cursor.fetchall()
                    return True, result
                else:
                    conn.commit()
                    affected_rows = cursor.rowcount
                    return True, affected_rows
    except Exception as e:
        print(f"Error executing query: {e}")
        return False

def fetch_one(query, params=None):
    
    success, result = execute_query(query, params)
    if success and result:
        return True, result[0]
    return success, None

def fetch_all(query, params=None):
    return execute_query(query, params)      



def main():
    conn = get_connection()

    if conn is not None:
        print("Connection to the database was successful!")
    
        query = "SELECT c.jedinstveni_identifikator, c.datum_registracije, c.starost, c.pol, c.bracno_stanje, MAX(p.datum) AS poslednja_posjeta FROM client_source c JOIN product_change_source p ON c.jedinstveni_identifikator = p.jedinstveni_identifikator GROUP BY c.jedinstveni_identifikator, c.datum_registracije, c.starost, c.pol, c.bracno_stanje;"
        success, result = execute_query(conn, query)
        print(result)
        
        if success:
            print("Test query executed successfully.")
        else:
            print("Error executing test query.")
        
        conn.close()
    else:
        print("Failed to connect to the database.")

if __name__ == "__main__":
    main()


   