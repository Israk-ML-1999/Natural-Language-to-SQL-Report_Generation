FROM python:3.12-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install required packages and dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libffi-dev \
    libssl-dev \
    python3-dev \
    musl-dev \
    net-tools \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY Requirements.txt /app/

# Install Python dependencies
RUN pip install --no-cache-dir -r Requirements.txt

# Copy the application code into the container
COPY . /app/

# Create required directories for outputs
RUN mkdir -p /app/reports /app/charts /app/temp

# Ensure proper permissions for all required folders
RUN chmod -R 777 /app/reports /app/charts /app/temp

# Expose port 8000 (for API/web server)
EXPOSE 8000

# Run the application with Uvicorn
# For standalone script:
# CMD ["python", "nl_to_sql_langgraph.py"]
# For FastAPI with Uvicorn:
CMD ["uvicorn", "web_server:app", "--host", "0.0.0.0", "--port", "8000"]

