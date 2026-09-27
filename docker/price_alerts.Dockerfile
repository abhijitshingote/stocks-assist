FROM python:3.11-slim

WORKDIR /app

ENV TZ=America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY price_alerts/requirements.txt ./price_alerts/requirements.txt
RUN pip install --no-cache-dir -r price_alerts/requirements.txt

COPY price_alerts ./price_alerts

ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "price_alerts.watcher"]
