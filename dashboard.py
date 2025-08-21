import streamlit as st
import pandas as pd
import pyodbc
#import seaborn as sns
#import matplotlib.pyplot as plt
from utils import convert_times, get_workout_count, get_count_and_delta
import altair as alt
import plotly.express as px
import plotly.io as pio
from datetime import datetime
pio.templates.default = "plotly_dark"

# Comment this out before deploying
driver_path = "/opt/homebrew/lib/libmsodbcsql.18.dylib"

############################################################################################
                                    # Page/DB Config #
############################################################################################

# The following Azure SQL Connection code is adapted from Streamlit's documentation on MS SQL:
# https://docs.streamlit.io/develop/tutorials/databases/mssql
# Initialize connection.
# Uses st.cache_resource to only run once.

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
        f"DRIVER={{{driver_path}}};"
        # Uncomment for deployment
        #"DRIVER={ODBC Driver 17 for SQL Server};"
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

# Additional queries

most_recent_query = """
    WITH MostRecent AS (
        SELECT TOP 1 workout_id, CAST(start_time AS DATETIME) AS start_time, CAST(end_time AS DATETIME) AS end_time
        FROM WORKOUTS
        ORDER BY start_time DESC
    )

    SELECT
        CONCAT(
            FORMAT(MostRecent.start_time, 'M/d/yyyy'),
            ' ',
            FORMAT(MostRecent.start_time, 'hh.mm tt')
        ) AS StartTime,
        CONCAT(
            FORMAT(MostRecent.end_time, 'M/d/yyyy'),
            ' ',
            FORMAT(MostRecent.end_time, 'hh.mm tt')
        ) AS EndTime,
        workouts.exercise_title,
        workouts.exercise_notes,
        workouts.set_weight_lbs,
        workouts.workout_type,
        workouts.workout_duration
    FROM workouts
    JOIN MostRecent ON workouts.workout_id = MostRecent.workout_id
"""

past_workouts_query = """
    SELECT
        COUNT(DISTINCT CASE WHEN start_time >= DATEADD(day, -30, GETDATE()) THEN workout_id END) AS past_month,
        COUNT(DISTINCT CASE WHEN start_time BETWEEN DATEADD(day, -60, GETDATE()) AND DATEADD(day, -30, GETDATE()) THEN workout_id END) AS prev_month
    FROM
        workouts
"""

most_recent_workout = run_query(most_recent_query)
past_workouts = run_query(past_workouts_query)
past_month_workout_count, delta_label = get_count_and_delta(past_workouts)

profile_pic_url = "https://d2l9nsnmtah87f.cloudfront.net/profile-images/dstorms-0af3c61a-c892-4a2c-9ad6-946589ad2789.jpg"

############################################################################################
                                        # Header #
############################################################################################

col3, col4 = st.columns([1, 5])
with col3:
    st.image(profile_pic_url, width=120, caption=f":green[**[Hevy Profile](https://hevy.com/user/dstorms)**] 🏋️‍♂️")
with col4:
    st.title("Daniel's Workout Dashboard")
    col41, col42= st.columns([3, 2])
    with col41:
        st.metric(label="Total Workouts 💪", value=get_workout_count())
    with col42:
        st.metric(
            label="Workouts Past Month 📅",
            value=past_month_workout_count,
            delta=delta_label
        )

############################################################################################
                                    # Data Manipulation #
############################################################################################

# Filter the dataframe by data
start_date, end_date = st.date_input("Date range", [df['start_time'].min(), df['start_time'].max()])
filtered_df = df[(df['start_time'] >= pd.to_datetime(start_date)) & (df['start_time'] <= pd.to_datetime(end_date))]

# Average workout duration per day
avg_duration = filtered_df.groupby(filtered_df['start_time'].dt.date)['workout_duration'].mean()

# Average volume by exercise title
exercise_avg_volume = filtered_df.groupby('exercise_title')['volume'].mean()
# Average volume by workout type
workout_avg_volume = filtered_df.groupby('workout_type')['volume'].mean()
# Total volume over time
volume_over_time = filtered_df.groupby(filtered_df['start_time'].dt.date)['volume'].sum().reset_index()
# Total volume by workout type
exercise_volume = filtered_df.groupby("workout_type")['volume'].sum().reset_index()

# Top exercises / progression
top_exercises = filtered_df['exercise_title'].value_counts().head(5).index
progression = filtered_df[filtered_df['exercise_title'].isin(top_exercises)]

############################################################################################
                                        # Graphics #
############################################################################################

col1, col2 = st.columns(2)

# First column
with col1:
    # Line Chart: Workout duration over time
    fig = px.line(
        avg_duration.reset_index(),
        x="start_time",
        y="workout_duration",
        title="Workout Duration Over Time",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Bar Chart: Workout Volume Over Time
    fig = px.bar(
        volume_over_time,
        x="start_time",
        y="volume",
        title="Total Workout Volume Over Time",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Pie Chart: Total volumne by workout type
    fig = px.pie(
        exercise_volume,
        names="workout_type",
        values="volume",
        title="Proportion of Total Volume by Exercise",
    )
    st.plotly_chart(fig, use_container_width=True)
# Second column
with col2:
    # Most Recent Workout
    # Create HTML string to be able to scroll through the results
    scrollable_html = '<div style="height: 400px; overflow-y: scroll;">'
    # Add the header
    scrollable_html += "<h3><em>Most Recent Workout</em></h3>"
    # Convert the date to separate parts, date and time
    dt = datetime.strptime(most_recent_workout.iloc[0]['StartTime'], "%m/%d/%Y %I.%M %p")
    # Part for the date
    date_part = f"{dt.month}/{dt.day}/{dt.year}"
    hour_12 = dt.hour % 12
    if hour_12 == 0:
        hour_12 = 12
    # Part for the time (converted to 12-hour format)
    time_part = f"{hour_12}:{dt.minute:02d} {dt.strftime('%p')}"
    # Add this to the HTML
    scrollable_html += f"<h4><em style='color: lightblue'>{most_recent_workout.iloc[0]['workout_type']}</em> on {date_part} at {time_part}</h4>"
    scrollable_html += "<hr>"
    grouped_exercises = most_recent_workout.groupby('exercise_title')
    for exercise_title, sets_df in grouped_exercises:
        # Bold exercise title
        scrollable_html += f"<strong>{exercise_title}</strong>"
        # Iterate through the sets for this exercise
        for i, row in enumerate(sets_df.itertuples(), 1):
            scrollable_html += f"<p>Set {i}: {round(row.set_weight_lbs)} lbs</p>"
        scrollable_html += "<hr>"
    # Close the HTML div
    scrollable_html += "</div>"
    st.markdown(scrollable_html, unsafe_allow_html=True)

    # Line Chart: Progression of Top Exercises Over Time
    fig = px.line(
        progression,
        x="start_time",
        y="weight_score",
        color="exercise_title",
        title="Weight Progression for Top Exercises",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Histograph: Workout duration
    fig = px.histogram(
        filtered_df,
        x="workout_duration",
        nbins=20,
        title="Distribution of Workout Duration (minutes)",
        template="plotly_dark"
    )
    st.plotly_chart(fig, use_container_width=True)
