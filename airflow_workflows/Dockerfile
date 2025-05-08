FROM apache/airflow:2.9.1

# Copy your project-level requirements.txt into the image
COPY requirements.txt /tmp/requirements.txt

# Install requirements
RUN pip install --no-cache-dir -r /tmp/requirements.txt
