import os
import requests
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from utils import connect_to_db, clean_workouts, get_workout_count, send_email, get_row_count, save_table_to_csv

load_dotenv()

'''
----------------------------------------

File to fetch workouts from HEVY API
and store them in an Azure SQL Database.

----------------------------------------

'''

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
                exercise_index = exercise['index']
                exercise_title = exercise['title']
                exercise_notes = exercise['notes']
                # For each set in the exercise
                for set in exercise['sets']:
                    set_index = set['index']
                    # The weight is in kg, so convert it to lbs
                    set_weight = np.round(set['weight_kg'] * 2.205, 2) if set['weight_kg'] is not None else None
                    set_reps = set['reps']
                    # For each set, create a new row in the DF
                    workouts.append({
                        "workout_id": workout_id,
                        "workout_title": workout_title,
                        "start_time": start_time,
                        "end_time": end_time,
                        "exercise_index": exercise_index,
                        "exercise_title": exercise_title,
                        "exercise_notes": exercise_notes,
                        "set_index": set_index,
                        "set_weight_lbs": set_weight,
                        "set_reps": set_reps
                    })
    return pd.DataFrame(workouts)

def create_table(conn, df: pd.DataFrame):
    cursor = conn.cursor()
    # Determine which columns will be strings/floats
    int_cols = df.select_dtypes(include=['int64']).columns.tolist()
    float_cols = df.select_dtypes(include=['float64']).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object']).columns.tolist()

    # Join the columns
    columns = ', '.join(
        [f"[{col}] NVARCHAR(MAX)" if col in categorical_cols
         else f"[{col}] FLOAT" if col in float_cols
         else f"[{col}] INT" if col in int_cols
         else f"[{col}] NVARCHAR(MAX)" # Default to string
         for col in df.columns]
    )
    try:
    # Check if the table exists already and if not create it with the columns listed above
        create_table_query = f"""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='workouts')
            CREATE TABLE workouts ({columns})
        """

        cursor.execute(create_table_query)
        conn.commit()
    except Exception as e:
        print("Error creating table:", e)
    finally:
        cursor.close()
        # Do not close connection here, it is closed in run_pipeline

def insert_sql(conn, df: pd.DataFrame):
    cursor = conn.cursor()

    try:
        # Loop through the dataframe and insert the rows into our table
        # Create placeholders (as good practice to avoid SQL injection threats)
        insert_placeholders = ', '.join(['?'] * len(df.columns))
        dup_check_placeholders = ' AND '.join([f"{col} = ?" for col in ['workout_id', 'exercise_title', 'set_index']])
        # Prepare column names
        columns = ', '.join([f"[{col}]" for col in df.columns])
        # Generate insert query
        insert_query = f"""
        INSERT INTO workouts ({columns})
        SELECT {insert_placeholders}
        WHERE NOT EXISTS (
            SELECT 1 FROM workouts
            WHERE {dup_check_placeholders}
        )
        """
        # Prepare values to be inserted into the database
        values = []
        for row in df.to_numpy():
            row_tuple = tuple(row)
            # Construct primary key to be used in duplicate check
            workout_id = row[df.columns.get_loc("workout_id")]
            exercise_title = row[df.columns.get_loc("exercise_title")]
            set_index = row[df.columns.get_loc("set_index")]
            # Append everything to values for the insert query
            values.append(row_tuple + (workout_id, exercise_title, set_index))

        # Change cursor settings to execute many rows fast
        cursor.fast_executemany = True
        # Execute the query with the current row's data
        cursor.executemany(insert_query, values)
        # Return the number of rows added
        conn.commit()
        print(f"Successfully inserted data.")

    except Exception as e:
        print("Error in connection:", e)
    finally:
        cursor.close()
        # Do not close connection here, it is closed in run_pipeline

def run_pipeline():
    # Fetch the workouts from the API
    workouts = get_all_workouts()

    # Clean the workouts
    cleaned_workouts = clean_workouts(workouts)

    # Connect to our database
    conn = connect_to_db()
    if conn is None:
        print("Failed to connect to the database.")
        return

    # Create the workouts table
    create_table(conn, cleaned_workouts)

    # Get the row count before insertion
    initial_row_count = get_row_count(conn)

    # Insert data into the workouts table
    insert_sql(conn, cleaned_workouts)

    # Save a backup of the table as CSV incase of DB issues
    save_table_to_csv(conn)

    # Get the row count after insertion
    final_row_count = get_row_count(conn)

    rows_added = final_row_count - initial_row_count

    # Send an email to notify of successful DB update
    send_email(rows_added)

    conn.close()
    
if __name__ == "__main__":
    run_pipeline()