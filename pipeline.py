import os
import requests
import csv
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from utils import connect_to_db

load_dotenv()

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

# Function to get number of workouts to determine how many pages to fetch
def get_workout_count():
    response = requests.get(f"{base_url}/workouts/count", headers=headers)
    if response.status_code != 200:
        print(f"Error fetching workout count: {response.status_code}")
        return 0
    data = response.json()
    return data['workout_count']

# Function to fetch all workouts from HEVY API
def get_all_workouts():
    # Page count is the ceiling of workout count divided by page size (10)
    # We divide it by page_size then add 1 if there's a remainder
    workout_count = get_workout_count()
    page_size = 10
    page_count = workout_count // 10 + (1 if workout_count % 10 > 0 else 0)
    workouts = []
    for page in range(1, page_count + 1):
        # Make the HTTP GET request to fetch workouts
        response = requests.get(f"{base_url}/workouts", 
                                        # Page: Current page number
                                params={"page": page,
                                        # Page size: Number of items retrieved per page (Max 10)
                                        "pageSize": page_size},
                                headers=headers)
        if response.status_code != 200:
            print(f"Error fetching workouts: {response.status_code}")
            break
        # Parse the JSON response
        data = response.json()
        if not data:
            break
        # We want one row per set, so we have to iterate through each workout and its exercises
        for workout in data['workouts']:
            workout_id = workout['id']
            workout_title = workout['title']
            start_time = workout['start_time']
            end_time = workout['end_time']
            # For each exercise in the workout
            for exercise in workout['exercises']:
                exercise_title = exercise['title']
                exercise_notes = exercise['notes']
                # For each set in the exercise
                for set in exercise['sets']:
                    set_index = set['index']
                    # The weight is in kg, so convert it to lbs
                    set_weight = set['weight_kg'] * 2.205 if set['weight_kg'] is not None else None
                    set_reps = set['reps']
                    # For each set, create a new row in the DF
                    workouts.append({
                        "workout_id": workout_id,
                        "workout_title": workout_title,
                        "start_time": start_time,
                        "end_time": end_time,
                        "exercise_title": exercise_title,
                        "exercise_notes": exercise_notes,
                        "set_index": set_index,
                        "set_weight_lbs": set_weight,
                        "set_reps": set_reps
                    })
    return pd.DataFrame(workouts)

def insert_sql(df: pd.DataFrame):
    # Try connecting to our database
    conn = connect_to_db()
    if conn is None:
        print("Failed to connect to the database.")
        return

    cursor = conn.cursor()

    try:
        # Create the table in our DB if it does not already exist

        # Determine which columns will be strings/floats
        num_cols = df.select_dtypes(include=['number']).columns.tolist()
        cat_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()

        # Fill the NaN values with None for SQL compatability
        df[num_cols] = df[num_cols].where(pd.notnull(df[num_cols]), None)
        
        # Join the columns
        columns = ', '.join(
            [f"[{col}] NVARCHAR(MAX)" if col in cat_cols else f"[{col}] FLOAT" for col in df.columns]
        )
        
        # Check if the table exists already and if not create it with the columns listed above
        create_table_query = f"""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='workouts' AND xtype='U')
            CREATE TABLE workouts ({columns})
        """

        cursor.execute(create_table_query)
        conn.commit()

        # Loop through the dataframe and insert the rows into our table
        for index, row in df.iterrows():
            print(f"Inserting row {index}")
            # Create placeholders (as good practice to avoid SQL injection threats)
            placeholders = ', '.join(['?'] * len(row))
            # Prepare column names
            columns = ', '.join([f"[{col}]" for col in df.columns])
            # Generate insert query
            insert_query = f"INSERT INTO workouts ({columns}) VALUES ({placeholders})"
            # Execute the query with the current row's data
            cursor.execute(insert_query, tuple(row))
        conn.commit()

        print("Successfully inserted data")

        cursor.close()
        conn.close()

    except Exception as e:
        print("Error in connection:", e)

def run_pipeline():
    workouts = get_all_workouts()
    insert_sql(workouts)

if __name__ == "__main__":
    run_pipeline()