import pyodbc
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

'''
----------------------------------------

File with shared utility functions.
(Or just functions that look messy
in my pipeline)

----------------------------------------

'''

# Helper function to connect to my Azure SQL DB
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

# Helper function to clean the DataFrame (Taking all the changes from EDA into account)
def clean_workouts(df):
    df = df.copy()

    mean_weight_by_exercise = df.groupby('exercise_title')['set_weight_lbs'].mean()
    df['set_weight_lbs'] = df['set_weight_lbs'].fillna(df['exercise_title'].map(mean_weight_by_exercise))
    df = df.dropna(subset=['set_weight_lbs'])

    def create_workout_type(title):
        title = title.lower()
        if 'lower' in title or 'leg' in title:
            return 'Lower'
        elif 'upper' in title:
            return 'Upper'
        elif 'pull' in title:
            return 'Pull'
        elif 'push' in title:
            return 'Push'
        elif 'full' in title:
            return 'Full Body'
        else:
            return 'Other'
    
    df.loc[:, 'workout_type'] = df['workout_title'].apply(create_workout_type)
    df.drop(columns=['workout_title'], inplace=True)
    # For some reason this isn't renaming it
    df.rename(columns={'workout_title': 'workout_type'}, inplace=True)
    return df

# Helper function to convert start and end times to datetime objects
def convert_times(df):
    df = df.copy()
    df['start_time'] = pd.to_datetime(df['start_time']).dt.tz_localize(None)
    df['end_time'] = pd.to_datetime(df['end_time']).dt.tz_localize(None)
    return df