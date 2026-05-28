# nyc-taxi-data-engineering-azure-synapse
This project showcases a complete end-to-end data engineering workflow built with Azure Synapse Analytics and the NYC Taxi Trips dataset. It includes data exploration, virtualization, ingestion, transformation, and reporting through the use of Serverless SQL, external data sources, views, stored procedures, CETAS, Parquet files, and Power BI.

<img width="1051" height="608" alt="252400938-8293544d-c6a4-46f6-b77b-4137b59d8693" src="https://github.com/user-attachments/assets/7a58b504-615a-44bb-94c2-c5912ebb23ce" />

## 1. Data Overview

The dataset used in this project is derived from New York City taxi trip records. In New York City, several types of taxi and for-hire vehicle services operate:

- **Yellow taxis**: Primarily pick up passengers within the inner city
- **Green taxis**: Typically serve the outer boroughs and may drop off passengers anywhere in the city
- **For-Hire Vehicles (FHV)**: Operate across all areas of the city
- **High-Volume For-Hire Vehicles (HVFHV)**: A subcategory of for-hire vehicles with higher trip volumes

## 2. Overview of NYC Taxi Data Files

This project uses a combination of file formats and reference datasets, including:

- **Taxi Zone** - CSV
- **Calendar** - CSV
- **Trip Type** - TSV
- **Rate Code** - JSON
- **Payment Type** - JSON
- **Vendor** - Quoted CSV
- **Trip Data** - Parquet, CSV, and Delta

<img width="593" height="329" alt="image" src="https://github.com/user-attachments/assets/00eba23a-e157-4aa7-949b-6c82aa0866c1" />


## 3. Solution Architecture

The solution is designed around Azure Synapse Serverless SQL and is organized into the following key stages:

- **Data Discovery**: Uses Serverless SQL to examine raw files and assess data structure and quality
- **Data Virtualization**: Uses external data sources and external file formats to enable simplified querying of files directly from storage
- **Data Ingestion**: Uses external tables, stored procedures, and views to ingest and expose partitioned data files
- **Data Transformation**: Uses T-SQL, CETAS, views, and stored procedures to cleanse, combine, aggregate, and persist analytical datasets in Parquet format
- **Reporting**: Uses Power BI dashboards to deliver insights into taxi demand, payment patterns, and operational performance

<img width="590" height="308" alt="image" src="https://github.com/user-attachments/assets/1af8997b-9aef-4454-af03-737f9a07ff0d" />


## 4. Project Requirements

### 4.1 Data Discovery

The data discovery phase is focused on understanding the structure, quality, and business relevance of the raw data.

**Key tasks include:**

- Identifying duplicate records
- Checking for missing values
- Detecting invalid or unexpected column values
- Joining data from multiple files
- Summarizing and aggregating data
- Applying basic transformations

**Path:** `SQL Scripts/discovery/`

#### Assignment: Cash vs Credit Card Trips by Borough

The objective of this assignment is to calculate the percentage of trips paid by cash and by credit card for each borough.

```sql
WITH v_payment_type AS
(
    SELECT
        CAST(JSON_VALUE(jsonDoc, '$.payment_type') AS SMALLINT) AS payment_type,
        CAST(JSON_VALUE(jsonDoc, '$.payment_type_desc') AS VARCHAR(15)) AS payment_type_desc
    FROM OPENROWSET(
        BULK 'payment_type.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '1.0',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0a'
    )
    WITH
    (
        jsonDoc NVARCHAR(MAX)
    ) AS payment_type
),
v_taxi_zone AS
(
    SELECT
        *
    FROM OPENROWSET(
        BULK 'taxi_zone.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    )
    WITH (
        location_id SMALLINT 1,
        borough VARCHAR(15) 2,
        zone VARCHAR(50) 3,
        service_zone VARCHAR(15) 4
    ) AS [result]
),
v_trip_data AS
(
    SELECT
        *
    FROM OPENROWSET(
        BULK 'trip_data_green_parquet/year=2021/month=01/**',
        FORMAT = 'PARQUET',
        DATA_SOURCE = 'nyc_taxi_data_raw'
    ) AS [result]
)
SELECT
    v_taxi_zone.borough,
    COUNT(1) AS total_trips,
    SUM(CASE WHEN v_payment_type.payment_type_desc = 'Cash' THEN 1 ELSE 0 END) AS cash_trips,
    SUM(CASE WHEN v_payment_type.payment_type_desc = 'Credit card' THEN 1 ELSE 0 END) AS card_trips,
    CAST(
        (SUM(CASE WHEN v_payment_type.payment_type_desc = 'Cash' THEN 1 ELSE 0 END) / CAST(COUNT(1) AS DECIMAL)) * 100
        AS DECIMAL(5, 2)
    ) AS cash_trips_percentage,
    CAST(
        (SUM(CASE WHEN v_payment_type.payment_type_desc = 'Credit card' THEN 1 ELSE 0 END) / CAST(COUNT(1) AS DECIMAL)) * 100
        AS DECIMAL(5, 2)
    ) AS card_trips_percentage
FROM v_trip_data
LEFT JOIN v_payment_type
    ON v_trip_data.payment_type = v_payment_type.payment_type
LEFT JOIN v_taxi_zone
    ON v_trip_data.PULocationId = v_taxi_zone.location_id
WHERE v_payment_type.payment_type_desc IN ('Cash', 'Credit card')
GROUP BY v_taxi_zone.borough
ORDER BY v_taxi_zone.borough;
```

**Output:**
<img width="1208" height="256" alt="image" src="https://github.com/user-attachments/assets/20cd8953-ee8e-468c-b344-66fa2fd21573" />


### 4.2 Data Virtualization

Data virtualization establishes a logical data layer that enables users to query files directly from storage without first building complex ETL pipelines.

#### Create External Data Source

```sql
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'nyc_taxi_src')
    CREATE EXTERNAL DATA SOURCE nyc_taxi_src
    WITH
    (
        LOCATION = 'Path'
    );
```

#### Create External File Format

```sql
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'csv_file_format')
    CREATE EXTERNAL FILE FORMAT csv_file_format
    WITH
    (
        FORMAT_TYPE = DELIMITEDTEXT,
        FORMAT_OPTIONS
        (
            FIELD_TERMINATOR = ',',
            STRING_DELIMITER = '"',
            FIRST_ROW = 2,
            USE_TYPE_DEFAULT = FALSE,
            ENCODING = 'UTF8',
            PARSER_VERSION = '2.0'
        )
    );
```

#### Full Example Using External Source and External File Format
<img width="257" height="190" alt="image" src="https://github.com/user-attachments/assets/53b1dbf2-1313-431b-9285-636e07361907" />

### 4.3 Data Ingestion

The data is stored across multiple partitioned folders, so the ingestion layer is designed to combine files from different paths and expose them through views. This approach simplifies querying while preserving partition columns such as `year` and `month` for efficient filtering.
<img width="1186" height="650" alt="image" src="https://github.com/user-attachments/assets/425df06e-97fc-4f5b-acc9-522524cedba3" />

### Create a View for Green Taxi Trip Data

```sql
DROP VIEW IF EXISTS silver.vw_trip_data_green;
GO

CREATE VIEW silver.vw_trip_data_green
AS
SELECT
    result.filepath(1) AS year,
    result.filepath(2) AS month,
    result.*
FROM OPENROWSET(
    BULK 'silver/trip_data_green/year=*/month=*/*.parquet',
    DATA_SOURCE = 'nyc_taxi_src',
    FORMAT = 'PARQUET'
)
WITH (
    vendor_id INT,
    lpep_pickup_datetime DATETIME2(7),
    lpep_dropoff_datetime DATETIME2(7),
    store_and_fwd_flag CHAR(1),
    rate_code_id INT,
    pu_location_id INT,
    do_location_id INT,
    passenger_count INT,
    trip_distance FLOAT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    ehail_fee INT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    payment_type INT,
    trip_type INT,
    congestion_surcharge FLOAT
) AS [result];
GO

SELECT TOP (100) *
FROM silver.vw_trip_data_green;
GO
```

### 4.4 Data Transformation

The transformation layer prepares the data for analytics and reporting.

**Key objectives include:**

- Joining the core information required for reporting
- Joining the core information required for analysis
- Querying ingested data using SQL
- Analyzing transformed data using T-SQL
- Storing transformed datasets in a columnar format such as Parquet

#### Business Requirement 1: Credit Card Payment Campaign

The business wants to encourage more customers to use credit card payments. This analysis focuses on:

- Trips paid by credit card versus cash
- Payment behavior on weekdays and weekends
- Payment behavior across boroughs

```sql
SELECT
    td.year,
    td.month,
    tz.borough,
    CONVERT(DATE, td.lpep_pickup_datetime) AS trip_date,
    cal.day_name AS trip_day,
    CASE WHEN cal.day_name IN ('Saturday', 'Sunday') THEN 'Y' ELSE 'N' END AS trip_day_weekend_ind,
    SUM(CASE WHEN pt.description = 'Credit card' THEN 1 ELSE 0 END) AS card_trip_count,
    SUM(CASE WHEN pt.description = 'Cash' THEN 1 ELSE 0 END) AS cash_trip_count
FROM silver.vw_trip_data_green td
JOIN silver.taxi_zone tz
    ON td.pu_location_id = tz.location_id
JOIN silver.calendar cal
    ON cal.date = CONVERT(DATE, td.lpep_pickup_datetime)
JOIN silver.payment_type pt
    ON td.payment_type = pt.payment_type
WHERE td.year = '2020'
  AND td.month = '01'
GROUP BY
    td.year,
    td.month,
    tz.borough,
    CONVERT(DATE, td.lpep_pickup_datetime),
    cal.day_name;
```

#### Business Requirement 2: Taxi Demand Analysis

The business wants to understand taxi demand patterns across boroughs, weekdays, weekends, and trip types.

The analysis includes:

- Overall taxi demand
- Demand by borough
- Demand by weekday and weekend
- Demand by trip type, such as Street-hail and Dispatch
- Trip distance, trip duration, and fare amount by day and borough

```sql
SELECT
    td.year,
    td.month,
    tz.borough,
    CONVERT(DATE, td.lpep_pickup_datetime) AS trip_date,
    cal.day_name AS trip_day,
    CASE WHEN cal.day_name IN ('Saturday', 'Sunday') THEN 'Y' ELSE 'N' END AS trip_day_weekend_ind,
    SUM(CASE WHEN pt.description = 'Credit card' THEN 1 ELSE 0 END) AS card_trip_count,
    SUM(CASE WHEN pt.description = 'Cash' THEN 1 ELSE 0 END) AS cash_trip_count,
    SUM(CASE WHEN tt.trip_type_desc = 'Street-hail' THEN 1 ELSE 0 END) AS street_hail_trip_count,
    SUM(CASE WHEN tt.trip_type_desc = 'Dispatch' THEN 1 ELSE 0 END) AS dispatch_trip_count,
    SUM(td.trip_distance) AS trip_distance,
    SUM(DATEDIFF(MINUTE, td.lpep_pickup_datetime, td.lpep_dropoff_datetime)) AS trip_duration,
    SUM(td.fare_amount) AS fare_amount
FROM silver.vw_trip_data_green td
JOIN silver.taxi_zone tz
    ON td.pu_location_id = tz.location_id
JOIN silver.calendar cal
    ON cal.date = CONVERT(DATE, td.lpep_pickup_datetime)
JOIN silver.payment_type pt
    ON td.payment_type = pt.payment_type
JOIN silver.trip_type tt
    ON td.trip_type = tt.trip_type
WHERE td.year = '2020'
  AND td.month = '01'
GROUP BY
    td.year,
    td.month,
    tz.borough,
    CONVERT(DATE, td.lpep_pickup_datetime),
    cal.day_name;
```

## 5. Reporting Requirements

The reporting layer is focused on delivering business-ready insights through Power BI.

**Reporting areas include:**

- Taxi demand analysis
- Credit card payment campaign analysis
- Operational reporting
- Borough, weekday, weekend, and trip type performance
- Aggregated metrics for distance, duration, and fare amount

#### Sample Dashboard
<img width="1261" height="714" alt="image" src="https://github.com/user-attachments/assets/e440d28c-7918-4b9f-9f94-39afc6392c25" />
<img width="1257" height="716" alt="image" src="https://github.com/user-attachments/assets/ca0e82b8-5d4d-40a6-98aa-0ae21571f951" />
