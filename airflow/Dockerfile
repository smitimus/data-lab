FROM apache/airflow:3.1.3

USER root

# Install system dependencies needed for dbt and other packages
# docker.io provides the Docker CLI for triggering Meltano via docker exec
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    build-essential \
    libssl-dev \
    libffi-dev \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    libjpeg-dev \
    libblas-dev \
    liblapack-dev \
    gfortran \
    curl \
    docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python packages from requirements.txt
# This is the recommended way to extend Airflow images
RUN pip install --no-cache-dir --user -r /requirements.txt

# Verify Airflow is still accessible after installing packages
RUN python -c "import airflow; print(f'Airflow version: {airflow.__version__}')" || \
    (echo "ERROR: Airflow module not found after package installation" && exit 1)

# Set working directory
WORKDIR /opt/airflow

