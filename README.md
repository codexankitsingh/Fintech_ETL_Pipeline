# Fintech Transaction ETL Pipeline

## Project Overview
This project processes financial transaction data to detect potential fraud and uploads the results to Google Cloud Storage (GCS). It is designed to run in a fully containerized environment using Docker and Apache Airflow.

## Tech Stack
-   **Language**: Python 3.9+
-   **Orchestration**: Apache Airflow (Dockerized)
-   **Containerization**: Docker & Docker Compose
-   **Cloud Storage**: Google Cloud Storage (GCS)
-   **Data Processing**: Pandas

## Prerequisites
-   **OS**: macOS
-   **Container Runtime**: Colima (Docker Desktop replacement)
-   **Package Manager**: Homebrew

## Structure
-   `dags/`: Airflow DAGs
-   `data/`: Local raw and processed data
-   `scripts/`: Python ETL scripts
-   `logs/`: Airflow logs
