FROM apache/airflow:2.9.1

# Switch to root to install system dependencies if needed
USER root

# (Optional: Install git or other system tools here if needed)

# Switch back to airflow user for pip install
USER airflow

# Copy requirements file (if we haven't mounted it, but best practice is COPY for build)
COPY requirements.txt /requirements.txt

# Install python packages
RUN pip install --no-cache-dir -r /requirements.txt
