import streamlit as st
import pandas as pd
import pyodbc
#import seaborn as sns
#import matplotlib.pyplot as plt
from utils import convert_times

'''
----------------------------------------

      File to host Interactive 
        StreamLit Dashboard

----------------------------------------

'''

# Uncomment for testing
# driver_path = "/opt/homebrew/lib/libmsodbcsql.18.dylib"

# The following Azure SQL Connection code is adapted from Streamlit's documentation on MS SQL:
# https://docs.streamlit.io/develop/tutorials/databases/mssql
# Initialize connection.
# Uses st.cache_resource to only run once.

# Page configuration
st.set_page_config(
   page_title="Workout Tracker",
   page_icon="🏋️‍♂️",
   layout="wide",
   initial_sidebar_state="expanded",
)

@st.cache_resource
def init_connection():
    return pyodbc.connect(
        # Uncomment for testing
        #f"DRIVER={st.secrets['driver_path']};"
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={st.secrets['server']};"
        f"DATABASE={st.secrets['database']};"
        f"UID={st.secrets['username']};"
        f"PWD={st.secrets['password']};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )

conn = init_connection()

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def run_query(query):
    return pd.read_sql_query(query, conn)

df = run_query("SELECT * FROM workouts;")
df = convert_times(df)

st.title("Workout Dashboard")

# Filter by date
start_date, end_date = st.date_input("Date range", [df['start_time'].min(), df['start_time'].max()])
filtered_df = df[(df['start_time'] >= pd.to_datetime(start_date)) & (df['start_time'] <= pd.to_datetime(end_date))]

# Calculate workout duration
filtered_df['workout_duration'] = (filtered_df['end_time'] - filtered_df['start_time']).dt.total_seconds() / 60
# Calculate volume
filtered_df['volume'] = filtered_df['set_weight_lbs'] * filtered_df['set_reps']

# Calculate average workout duration
avg_duration = filtered_df.groupby(filtered_df['start_time'].dt.date)['workout_duration'].mean()
st.line_chart(avg_duration)

# Calculate average volume by exercise type
avg_volume = filtered_df.groupby('exercise_title')['volume'].mean()
st.bar_chart(avg_volume)

# Workout type distribution
workout_counts = filtered_df['workout_type'].value_counts()
st.bar_chart(workout_counts)

# Top exercises by weight
top_exercises = filtered_df.groupby('exercise_title')['set_weight_lbs'].mean().sort_values(ascending=False).head(10)
st.bar_chart(top_exercises)
