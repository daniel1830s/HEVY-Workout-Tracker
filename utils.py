import pyodbc
import os
from dotenv import load_dotenv
import pandas as pd
import requests

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

# Helper function to clean the DataFrame (Combining all the cleaning from EDA)
def clean_workouts(df):
    df = df.copy()
    # Impute missing weights with the mean weight for each exercise
    mean_weight_by_exercise = df.groupby('exercise_title')['set_weight_lbs'].mean()
    df['set_weight_lbs'] = df['set_weight_lbs'].fillna(df['exercise_title'].map(mean_weight_by_exercise))
    # Drop remaining rows with missing weights
    df = df.dropna(subset=['set_weight_lbs'])
    # Normalize workout titles by creating workout type column
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
        
    # Replace workout title with workout type
    df.loc[:, 'workout_type'] = df['workout_title'].apply(create_workout_type)
    df.drop(columns=['workout_title'], inplace=True)
    df.rename(columns={'workout_title': 'workout_type'}, inplace=True)

    # Create a volume column for convenience
    df['volume'] = df['set_weight_lbs'] * df['set_reps']

    # Create a workout duration column in minutes
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['end_time'] = pd.to_datetime(df['end_time'])
    df['workout_duration'] = (df['end_time'] - df['start_time']).dt.total_seconds() / 60

    # Remove outliers from set weight lbs
    def filter_iqr(x):
        q1 = x.quantile(0.25)
        q3 = x.quantile(0.75)
        iqr = q3 - q1
        # Return boolean series for masking
        return (x >= q1 - 1.5*iqr) & (x <= q3 + 1.5*iqr)
    
    # Apply IQR filtering to remove outliers in set_weight_lbs for each exercise
    df = df.groupby('exercise_title').apply(lambda g: g[filter_iqr(g['set_weight_lbs'])]).reset_index(drop=True)

    # Further normalize weights by creating a z-score column for each exercise
    df['weight_score'] = df.groupby('exercise_title')['set_weight_lbs'].transform(
        lambda x: (x - x.mean()) / x.std()
    )

    # Fill remaining scores with 0
    df['weight_score'] = df['weight_score'].fillna(0)

    # Round these three columns to 2 decimal places
    df = df.round({
        'volume': 2,
        'workout_duration': 2,
        'weight_score': 2
    })

    return df

# Helper function to convert start and end times to datetime objects
def convert_times(df):
    df = df.copy()
    df['start_time'] = pd.to_datetime(df['start_time']).dt.tz_localize(None)
    df['end_time'] = pd.to_datetime(df['end_time']).dt.tz_localize(None)
    return df

# Function to get number of workouts to determine how many pages to fetch
def get_workout_count():
    # Declare environment variables
    API_KEY = os.getenv('HEVY_API_KEY')
    UID = os.getenv('UID')
    PSWD = os.getenv('PSWD')

    # Initialize HEVY API setup
    headers = {
        "accept": "application/json",
        "api-key": API_KEY
    }

    base_url = "https://api.hevyapp.com/v1"

    response = requests.get(f"{base_url}/workouts/count", headers=headers)
    if response.status_code != 200:
        print(f"Error fetching workout count: {response.status_code}")
        return 0
    data = response.json()
    return data['workout_count']