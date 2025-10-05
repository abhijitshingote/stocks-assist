FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Set timezone to Eastern Time
ENV TZ=America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Copy pyproject.toml and install dependencies
COPY pyproject.toml ./
RUN pip install --no-cache-dir -e .

# Copy frontend code
COPY frontend ./frontend
COPY user_data ./user_data

# Expose port
EXPOSE 5000

# Set environment variables
ENV FLASK_APP=frontend/app.py
ENV FLASK_ENV=development
ENV PYTHONUNBUFFERED=1

# Run the frontend application
CMD ["python", "frontend/app.py"]

