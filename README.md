# Workout Tracker Pipeline

This project fetches workout data from the HEVY API, processes it, and stores it in an Azure SQL Database. It also allows you to perform exploratory data analysis (EDA) and schedule the pipeline to run automatically.

The web-app is hosted on: https://weightlifting.streamlit.app/

---

## **Features**

* Fetch workouts and exercises from HEVY API
* Clean and preprocess data (e.g., impute missing weights, categorize workouts)
* Store data in Azure SQL Database
* Perform EDA and generate summary statistics
* Schedule daily automatic pipeline execution

---

## **Requirements**

* Python 3.10+
* Packages:

  ```bash
  pip install pandas numpy requests pyodbc python-dotenv schedule plotly streamlit statsmodel
  ```
* An Azure SQL Database instance
* HEVY API key

---

## **Environment Variables**

Create a `.env` file in the project root with the following:

```
HEVY_API_KEY=your_hevy_api_key
UID=your_database_username
PSWD=your_database_password
GMAIL_PASSWORD="your gmai lpas swor"
```

---

## **Usage**

### **1. Fetch and store workouts**

Run the pipeline script:

```bash
python pipeline.py
```

This will:

1. Fetch all workouts from HEVY API
2. Clean and transform the data
3. Insert it into the `workouts` table in your Azure SQL Database

---

### chedule daily execution**

Use `schedule` library:

```bash
python run_daily.py
```

* Run in background:

  ```bash
  nohup python3 run_daily.py &
  ```

---

### **Database Table Schema**

After preprocessing, the table `workouts` will have:

| Column Name       | Type          |
| ----------------- | ------------- |
| workout\_id       | INT           |
| workout\_type     | NVARCHAR(MAX) |
| start\_time       | DATETIME      |
| end\_time         | DATETIME      |
| exercise\_title   | NVARCHAR(MAX) |
| exercise\_notes   | NVARCHAR(MAX) |
| set\_index        | INT           |
| set\_weight\_lbs  | FLOAT         |
| set\_reps         | INT           |
| workout\_type     | NVARCHAR(MAX) |
| volume            | FLOAT         |
| workout\_duration | FLOAT         |
| weight\_score     | FLOAT         |

---

### **Notes**

* Missing weights are imputed by the mean weight per exercise.
* Workouts are categorized into `Upper`, `Lower`, `Pull`, `Push`, `Full Body`, and `Other`.
