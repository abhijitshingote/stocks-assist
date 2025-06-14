FROM python:3.9-slim

WORKDIR /app

# Install system dependencies including cron and timezone data
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    cron \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Set timezone to Eastern Time
ENV TZ=America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Make the daily price update script executable
RUN chmod +x daily_price_update.py

# Make the startup script executable
RUN chmod +x start.sh

# Create log file for cron
RUN touch /var/log/cron.log

# Expose port
EXPOSE 5000

# Set environment variables
ENV FLASK_APP=app.py
ENV FLASK_ENV=development

# Command to run both cron and the application
CMD ["./start.sh"] 