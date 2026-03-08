# Use a lightweight Python image
FROM python:3.11-slim

# Set working directory in the container
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app contents directly into /app
COPY app/ .

# Default command
CMD ["python", "main.py"]
