FROM apache/airflow:3.3.0

# Copy the requirements file directly
COPY requirements.txt .

# Install packages without saving heavy installer files
RUN pip install --no-cache-dir -r requirements.txt
