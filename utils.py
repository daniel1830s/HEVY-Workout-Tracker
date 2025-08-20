import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()

def connect_to_db():
    # Setting up DB conection
    driver_path = "/opt/homebrew/lib/libmsodbcsql.18.dylib"
    server = "tcp:stormsdb.database.windows.net,1433"
    database = "DanielDB"
    UID = os.getenv('UID')
    PSWD = os.getenv('PSWD')

    try:
        # Connection string for my Azure SQL Database
        conn = pyodbc.connect(
            f"DRIVER={{{driver_path}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={UID};"
            f"PWD={PSWD};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )
        return conn
    except Exception as e:
        print("Error in connection:", e)
        return None