# Project Overview
This project implements an end-to-end data pipeline using Apache Airflow to automate the ingestion, extraction, transformation, and storage of Wikipedia pageview data.
It was developed as part of a CDE Bootcamp task on Data Workflow Orchestration with Apache Airflow, focusing on applying Airflow to a practical, real-world data engineering scenario.

## Peline automates:

- Downloading Wikipedia hourly pageview data in compressed format (.gz)

- Extracting and filtering the data for specific companies

- Loading the processed data into a PostgreSQL database

- Cleaning up temporary files after execution

## Project Scenario
You were hired as a data engineer by a consulting firm to support the development of a stock market prediction tool called CoreSentiment.
The team aims to use Wikipedia pageview counts as a proxy for market sentiment, assuming that higher page views for a company indicate positive investor interest.

To validate this idea, the project retrieves and analyzes the hourly Wikipedia pageviews of five major tech companies:

1. Google

2. Facebook

3. Apple

4. Amazon

5. Microsoft

## Overview of the ETL Steps
- Create table - Creates the table where the data will  be residing

- Setup Environment – Creates directories for storing data files.

- Download Data – Fetches the .gz file for the specified date and hour.

- Extract File – Decompresses the .gz file into a text file.

- Filter & Transform – Extracts records for the five target companies and saves them as a CSV file.

- Load to PostgreSQL – Inserts the filtered CSV data into a PostgreSQL database table.

- Cleanup – Removes temporary files after successful execution.

## Dag Structure

![Dag Structure](/dags/page_counts/images/dag_struture.png)