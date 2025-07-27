# Airflow-Weather-Data-Pipeline
# Building an Airflow Weather Data Pipeline with MinIO Storage

## Introduction to Medium

Medium is a vibrant platform where writers, developers, and enthusiasts share knowledge, experiences, and tutorials with a global audience. Whether you're diving into data engineering, machine learning, or personal storytelling, Medium offers a space to publish accessible, high-quality content that inspires and educates. In this article, I'll walk you through a practical data engineering project: an Apache Airflow pipeline that fetches, transforms, and stores weather data in MinIO, a powerful S3-compatible object storage system. This project is perfect for data engineers looking to automate data workflows and integrate with modern storage solutions.

## Project Overview

This project creates an automated data pipeline using Apache Airflow to retrieve real-time weather data for Portland, Oregon, from the OpenWeatherMap API, transform it into a structured format, and store it as CSV files in a MinIO bucket. The pipeline runs daily, leveraging Airflow's scheduling capabilities, and uses a temporary folder (`~/airflow/tmp`) to stage data before uploading it to MinIO. The setup spans a Windows machine running MinIO and an Ubuntu environment (via WSL) hosting Airflow, showcasing cross-platform integration.

### Objectives

- **Fetch Data**: Query the OpenWeatherMap API for Portland's weather.
- **Transform Data**: Convert temperatures from Kelvin to Fahrenheit and structure the data using Pandas.
- **Store Data**: Save the transformed data as CSV files in MinIO's `weatherapiairflowyoutubebucket-yml` bucket.
- **Automate**: Use Airflow to schedule and orchestrate the pipeline.

## Prerequisites

Before diving in, ensure you have:

- **Ubuntu** (via WSL or standalone): For running Airflow.
- **Windows**: For running MinIO server.
- **Python 3.10+**: With a virtual environment for Airflow.
- **Dependencies**: `apache-airflow`, `apache-airflow-providers-http`, `apache-airflow-providers-amazon`, `pandas`, `boto3`.
- **MinIO**: Installed on Windows with a bucket named `weatherapiairflowyoutubebucket-yml`.
- **OpenWeatherMap API Key**: Sign up at [openweathermap.org](https://openweathermap.org) to get an API key.

## Setup Instructions

### Step 1: Set Up Ubuntu Environment

1. **Install Ubuntu (if using WSL)**:
   ```bash
   wsl --install -d Ubuntu

## Set Up Ubuntu Environment

### Update Ubuntu and install dependencies
```bash
sudo apt update
sudo apt install python3-pip python3.10-venv
