#  STEDI Human Balance Analytics

## 📝 Problem Statement

The STEDI Team has been hard at work developing a **hardware STEDI Step Trainer** that:

- 🦶 Trains the user to do a STEDI balance exercise  
- 🧭 Has sensors on the device that collect data to train a machine-learning algorithm to detect steps  
- 📱 Has a companion mobile app that collects customer data and interacts with the device sensors

STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance.  

The **Step Trainer** is essentially a **motion sensor** that records the distance of objects detected.  
The **mobile app** uses the phone’s **accelerometer** to detect motion in the X, Y, and Z directions.

The STEDI team wants to use this motion sensor data to train a **machine learning model** to detect steps accurately in real time.  

However, **privacy is a primary consideration** — only customers who have agreed to share their data for research can have their Step Trainer and accelerometer data included in the training dataset.

---

## 📌 Project Description

In this project, I extracted data produced by the **STEDI Step Trainer sensors** and the **mobile app**, and curated them into a **data lakehouse solution on AWS**.  

The goal is to provide **clean, trusted, and well-structured data** for **data scientists** to train machine learning models that can accurately detect human steps.

The Data Lake solution is built using:
- 🪣 **AWS S3** – to store landing, trusted, and curated datasets  
- 🧰 **AWS Glue** – for ETL jobs and schema crawlers  
- 🐍 **Python & Spark** – for transformations and deduplication  
- 🔍 **Amazon Athena** – to query and validate semi-structured data

The infrastructure consists of:
1. **Landing Zone** – Raw sensor & customer data.  
2. **Trusted Zone** – Filtered data for customers who agreed to share their information.  
3. **Curated Zone** – Aggregated and cleaned datasets ready for machine learning training.

---


## 📂 Project Structure
```
├── Athena_Query_Screenshots/ # Screenshots of Athena query results
├── Glue_ETL_Scripts/ # Python scripts from Glue Studio jobs
├── SQL_DDL_Landing/ # SQL DDLs defining landing tables
├── Glue_Studio_Screenshots/ # Visual job flow screenshots in Glue Studio
└── README.md # This document
```

---

## 🚀 Workflow & Logic

### 1. Landing Zone  
- Raw JSON files are uploaded to S3 (“landing” folders).  
- Tables for landing data are defined manually in Glue (with correct types).  
- Verified via Athena:
  - `customer_landing` → **956** rows  
  - `accelerometer_landing` → **81,273** rows  
  - `step_trainer_landing` → **28,680** rows  

---

### 2. Trusted Zone  
- **customer_landing_to_trusted**: filters customers with `shareWithResearchAsOfDate` not null, drops duplicates by email.  
- **accelerometer_landing_to_trusted**: inner joins `customer_trusted` on `email` and keeps only accelerometer fields.  
- **step_trainer_landing_to_trusted**: similar process for step-trainer data.  
- Verified via Athena:
  - `customer_trusted` → **482** rows  
  - `accelerometer_trusted` → **40,981** rows  
  - `step_trainer_trusted` → **14,460** rows  


---

### 3. Curated Zone & ML Dataset  
- **customer_trusted_to_curated**: join `customer_trusted` with `accelerometer_trusted` on `email`, retaining only customer fields.  
- **machine_learning_curated**: join `step_trainer_trusted` and `accelerometer_trusted` on matching timestamps (and serial number if used).  
- Verified via Athena:
  - `customer_curated` → **482** rows  
  - `machine_learning_curated` → **43,681** rows

### 4. Data Join Flow
```
Landing Zone
 ├─ customer_landing 
 │       └─ filter → customer_trusted
 ├─ accelerometer_landing 
 │       └─ join with customer_trusted → accelerometer_trusted
 └─ step_trainer_landing
         └─ join with customer_trusted → step_trainer_trusted

Trusted Zone → Curated Zone
 ├─ customer_trusted + accelerometer_trusted → customer_curated
 └─ step_trainer_trusted + accelerometer_trusted → machine_learning_curated
```
---

## 📊 Athena Queries Used

Here are sample Athena queries along with results (screenshots provided):

```sql
SELECT COUNT(*) FROM customer_landing;
SELECT COUNT(*) FROM accelerometer_landing;
SELECT COUNT(*) FROM step_trainer_landing;

SELECT COUNT(*) FROM customer_trusted;
SELECT COUNT(*) FROM accelerometer_trusted;
SELECT COUNT(*) FROM step_trainer_trusted;

SELECT COUNT(*) FROM customer_curated;
SELECT COUNT(*) FROM machine_learning_curated;

```

## 📋 Project Rubric

### Landing Zone
- **Use Glue Studio to ingest data from S3**  
  Glue jobs: `customer_landing_to_trusted.py`, `accelerometer_landing_to_trusted.py`, `step_trainer_landing_to_trusted.py`  

- **Manually create a Glue Table**  
  SQL DDL scripts: `customer_landing.sql`, `accelerometer_landing.sql`, `step_trainer_landing.sql`  

- **Query Landing Zone in Athena**  
  - `customer_landing` → 956 rows  
  - `accelerometer_landing` → 81,273 rows  
  - `step_trainer_landing` → 28,680 rows  
  - Check blank `shareWithResearchAsOfDate`  

---

### Trusted Zone
- **Dynamic schema updates in Glue Studio**  
  Option: “Create table in Data Catalog and update schema/add new partitions” = True  

- **Query Trusted Tables in Athena**  
  - `customer_trusted` → 482 rows (no blank `shareWithResearchAsOfDate`)  
  - `accelerometer_trusted` → 40,981 rows  
  - `step_trainer_trusted` → 14,460 rows  

- **Filter PII**  
  Drop rows with blank `shareWithResearchAsOfDate`  

- **Join Privacy Tables**  
  Accelerometer data joined with `customer_trusted` by email  

---

### Curated Zone
- **Join Trusted Data**  
  - `customer_trusted_to_curated.py`: join customer with accelerometer by email  
  - `step_trainer_trusted.py`: join step trainer with `customer_curated` by serial numbers  
  - `machine_learning_curated.py`: join step trainer with accelerometer by timestamp  

- **Query Curated Tables in Athena**  
  - `customer_curated` → 482 rows  
  - `machine_learning_curated` → 43,681 rows  

---

### Tips & Hints
- Prefer **SQL Query nodes** for consistent joins  
- Delete S3 files and Athena tables when updating ETLs  
- Use **Data Preview** to validate row counts    

