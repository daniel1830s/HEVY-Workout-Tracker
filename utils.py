import os
from dotenv import load_dotenv
import pandas as pd
import requests
import streamlit as st
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import psycopg2

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
    try:
        # Connection string for my Azure SQL Database
        conn = psycopg2.connect(
            host='localhost',
            database='airflow',
            user='daniel',
            password='postgres',
            port=5432
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

# Helper function to get number of workouts to determine how many pages to fetch
def get_workout_count():
    # Declare environment variables
    API_KEY = st.secrets['hevy_api_key']

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

# Helper function determine the % change in workouts between past month and previous month
def get_count_and_delta(past_workouts):
    # Get the past and previous month workout counts
    past_month = past_workouts.loc[0, 'past_month'] if 'past_month' in past_workouts.columns else 0
    prev_month = past_workouts.loc[0, 'prev_month'] if 'prev_month' in past_workouts.columns else 0

    delta_label = ""
    # If there were no workouts in the previous month
    if prev_month == 0:
        # We are 100% up unless
        if past_month > 0:
            delta_label = "100% up"
        # No workouts in the past month in which case both are 0
        else:
            delta_label = "0%"
    else:
        # Otherwise calculate normally
        percent_change = ((past_month - prev_month) / prev_month) * 100
        delta_label = f"{percent_change:.1f}%"
    return past_month, delta_label

# Helper function to send email notification when pipeline is ran
def send_email(rows_added):
    email = "daniel.1830.s@gmail.com"
    password = os.getenv('GMAIL_PASSWORD')
    msg = MIMEMultipart()
    msg['From'] = email
    msg['To'] = email
    msg['Subject'] = "Pipeline Run Succeeded! 🎉"
    if rows_added > 0:
        body = f"{rows_added} rows were added to the database ✅"
    else:
        body = "No new rows were added to the database ❌ Maybe you forgot to work out yesterday?"
    msg.attach(MIMEText(body, 'plain'))
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(email, password)
        text = msg.as_string()
        server.sendmail(email, email, text)
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")
    finally:
        server.quit()

# Helper function to get the number of rows in the database
def get_row_count(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM workouts")
        count = cursor.fetchone()[0]
        return count
    except Exception as e:
        print(f"Error fetching row count: {e}")
        return 0
    finally:
        cursor.close()

def save_table_to_csv(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM workouts")
        rows = cursor.fetchall()
        if not rows:
            print("No data found in the workouts table")
            return
        columns = [column[0] for column in cursor.description]
        df = pd.DataFrame.from_records(rows, columns=columns)
        df.to_csv("workouts_backup.csv", index=False)
        print("Workouts table saved to workouts_backup.csv")
    except Exception as e:
        print(f"Error saving workouts table to CSV: {e}")
    finally:
        cursor.close()