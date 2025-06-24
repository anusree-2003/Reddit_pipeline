# 🚀 Reddit Data Pipeline

An end-to-end cloud-native pipeline to fetch Reddit posts, store raw data in Amazon S3, clean it with AWS Glue, store it in Amazon Redshift, and visualize it using Power BI. The entire flow is automated.

---

## 🧱 Architecture

![Architecture Diagram](./redditetl.jpeg)

This pipeline automates Reddit data collection, cleaning, and reporting:

- Extracts Reddit posts using Python scripts
- Orchestrates  with Apache Airflow
- Uses AWS S3 for both raw and cleaned data storage
- Cleans data using AWS Glue (PySpark)
- Loads data into Amazon Redshift for structured analytics
- Visualizes insights in Power BI

---

## 💻 Tech Stack

| Tool / Service         | Description                                      
|------------------------|--------------------------------------------------
| 🐍 Python              | Reddit data extraction using PRAW                
| 🛠 Apache Airflow      | DAG-based scheduling and workflow orchestration   
| ☁️ Amazon S3          | Raw and cleaned data lake                          
| 🔄 AWS Glue            | Data transformation using PySpark                 
| 🧮 Amazon Redshift     | Cloud data warehouse for BI                       
| 📊 Power BI            | Business Intelligence dashboard                  

---

## ✨ Features

- 🔁 Automated Reddit post extraction using PRAW
- ☁️ Serverless data storage in AWS S3
- 🧹 Scalable data transformation using AWS Glue
- 🧠 Redshift COPY for quick data loading
- 📉 Visual analytics with Power BI


---

## 📥 Data Flow

Reddit API → Airflow → S3 (raw) →crawler - Glue (clean) - crawler → S3 (cleaned) → Redshift → Power BI


## 🧠 Overview

This project is a scalable Reddit Data Pipeline built with cloud-first and modular principles. Reddit posts are extracted using Python and the PRAW API, then stored as raw JSON in Amazon S3. AWS Glue cleans and deduplicates the data, writing the cleaned output back to S3. The cleaned data is then copied into Amazon Redshift using the COPY command for further analysis.

The process is fully automated using Apache Airflow DAGs and triggers. Analysts can connect Power BI directly to Redshift and build interactive reports and dashboards on Reddit activity and post metadata.

 ## 📌 Conclusion
 
The project showcases an end-to-end automated Reddit data pipeline using modern data engineering tools like Airflow, AWS Glue, S3, and Redshift. The pipeline is modular, serverless, and scalable, making it suitable for real-time data operations and reporting.

## 🔑 Highlights:

End-to-end automation 

Serverless transformation using Glue

Cost-effective data storage with S3

Centralized analytics using Redshift + Power BI


