# 🚗 Car Rental Data Pipeline (Batch SCD2 Ingestion)

## 📌 Project Overview
This project implements a **Car Rental Data Pipeline** using **Apache Airflow, PySpark, Google Cloud Storage (GCS), Dataproc, and Snowflake**.  
The pipeline handles **Slowly Changing Dimension Type 2 (SCD2)** ingestion, transformations, and fact table creation for analytical reporting.  

---

## ⚙️ Architecture
1. **Data Sources**
   - Daily customer data (`customer_daily_data`)
   - Daily booking data (`booking_daily_data`)
   - Spark job & Snowflake JARs stored in GCS

2. **Airflow Orchestration**
   - Manages pipeline execution
   - Performs **SCD2 merge** on `customer_dim`
   - Triggers **PySpark job** on **Dataproc Hadoop cluster** (2 worker nodes)

3. **PySpark Processing**
   - Reads raw JSON booking data from GCS
   - Validates & transforms records
   - Derives new attributes:
     - Rental duration in days
     - Total rental amount
     - Average daily rental amount
     - Long rental flag (>7 days)
   - Joins with dimension tables in Snowflake to fetch surrogate keys
   - Loads data into **`rentals_fact`**

4. **Snowflake Data Warehouse**
   - Stores dimension & fact tables:
     - `customer_dim` (SCD2 managed in Airflow)
     - `car_dim`
     - `location_dim`
     - `date_dim`
     - `rentals_fact`
   - Integrates with GCS via **External Stage & Storage Integration**

---

## 🛠️ Tech Stack
- **Orchestration**: Apache Airflow (Composer)
- **Processing**: PySpark on Google Dataproc
- **Storage**: Google Cloud Storage (GCS)
- **Warehouse**: Snowflake
- **Integration**: Snowflake External Stage with GCS

---

## 🚀 Pipeline Flow
1. Customer & booking data arrives in GCS buckets daily.
2. Airflow:
   - Runs **SCD2 merge** on `customer_dim` (upserts new & updated customers).
   - Submits PySpark job to Dataproc cluster.
3. PySpark:
   - Validates and transforms booking data.
   - Joins with dimension tables.
   - Loads enriched data into Snowflake fact table `rentals_fact`.
4. Snowflake:
   - Provides analytical-ready star schema (`dimensions + fact`).

---

## 📊 Example Tables
- **Dimension Tables**
  - `customer_dim`: Handles history with SCD2 (`effective_date`, `end_date`, `is_current`)
  - `car_dim`: Car details
  - `location_dim`: Rental pickup/dropoff locations
  - `date_dim`: Calendar table

- **Fact Table**
  - `rentals_fact`: Stores transactions enriched with surrogate keys & derived metrics

---

## ▶️ How to Run
1. **Deploy DAG in Airflow**
   - Place `car_rental_data_pipeline.py` inside `dags/` folder.
   - Configure Airflow connection `snowflake_conn_v2`.

2. **Prepare Spark Job**
   - Upload `spark_job.py` and Snowflake JARs to GCS bucket.
   - Ensure Dataproc cluster (`hadoop-cluster-new`) is running.

3. **Setup Snowflake**
   - Run `schema_setup.sql` to create dimensions, fact table, and GCS integration.
   - Verify external stage points to the correct GCS path.

4. **Trigger DAG**
   - Pass execution date (e.g., `20240703`) or allow Airflow default (`ds_nodash`).
   - DAG will:
     - Perform SCD2 merge on `customer_dim`
     - Insert new customer records
     - Submit PySpark job for fact table load

---

## 🔮 Future Enhancements
- Implement **SCD2 merge for booking_dim** as well.
- Add **data quality checks** in Airflow (using Great Expectations or custom PythonOperators).
- Automate **date_dim population** instead of manual inserts.
- Use **CI/CD pipeline** for deployment of DAGs and Spark jobs.

---

## 👨‍💻 Author
**Kushaj Solanki**  
Data Engineering Project | 2025
