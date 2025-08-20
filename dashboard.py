import streamlit as st
import pandas as pd
#import seaborn as sns
#import matplotlib.pyplot as plt
from utils import connect_to_db, convert_times

# Connect to Azure SQL
conn = connect_to_db()
df = pd.read_sql("SELECT * FROM workouts", conn)
df = convert_times(df)

st.title("Workout Dashboard")

# Filter by date
start_date, end_date = st.date_input("Date range", [df['start_time'].min(), df['start_time'].max()])
filtered_df = df[(df['start_time'] >= pd.to_datetime(start_date)) & (df['start_time'] <= pd.to_datetime(end_date))]

# Average workout duration
filtered_df['workout_duration'] = (filtered_df['end_time'] - filtered_df['start_time']).dt.total_seconds() / 60
avg_duration = filtered_df.groupby(filtered_df['start_time'].dt.date)['workout_duration'].mean()
st.line_chart(avg_duration)

# Workout type distribution
workout_counts = filtered_df['workout_type'].value_counts()
st.bar_chart(workout_counts)

# Top exercises by weight
top_exercises = filtered_df.groupby('exercise_title')['set_weight_lbs'].mean().sort_values(ascending=False).head(10)
st.bar_chart(top_exercises)
