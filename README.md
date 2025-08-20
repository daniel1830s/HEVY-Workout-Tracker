# Workout Tracker Pipeline

This project fetches workout data from the HEVY API, processes it, and stores it in an Azure SQL Database. It also allows you to perform exploratory data analysis (EDA) and schedule the pipeline to run automatically.

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
  pip install pandas numpy requests pyodbc python-dotenv schedule seaborn matplotlib
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

### **2. Perform EDA in a Jupyter Notebook**

* Import your cleaning functions and fetch data:

```python
import pandas as pd
from utils import get_all_workouts, clean_workouts

workouts = get_all_workouts()
cleaned_workouts = clean_workouts(workouts)
```

* Perform plots and analysis, e.g., average workout duration, sets/reps per exercise, etc.

---

### **3. Schedule daily execution**

Use `schedule` library:

```bash
python run_daily.py
```

Example scheduler in `run_daily.py`:

```python
import schedule
import time
from pipeline import run_pipeline

schedule.every().day.at("06:00").do(run_pipeline)

while True:
    schedule.run_pending()
    time.sleep(60)
```

* Adjust the time as needed.
* Run in background:

  ```bash
  nohup python3 run_daily.py &
  ```

---

### **4. Database Table Schema**

After preprocessing, the table `workouts` will have:

| Column Name      | Type          |
| ---------------- | ------------- |
| workout\_id      | INT           |
| workout\_title   | NVARCHAR(MAX) |
| workout\_type    | NVARCHAR(MAX) |
| start\_time      | DATETIME      |
| end\_time        | DATETIME      |
| exercise\_title  | NVARCHAR(MAX) |
| exercise\_notes  | NVARCHAR(MAX) |
| set\_index       | INT           |
| set\_weight\_lbs | FLOAT         |
| set\_reps        | INT           |

---

### **Notes**

* Missing weights are imputed by the mean weight per exercise.
* Workouts are categorized into `Upper`, `Lower`, `Pull`, `Push`, `Full Body`, and `Other`.
