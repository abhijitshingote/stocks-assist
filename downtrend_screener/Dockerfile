FROM python:3.12-slim
WORKDIR /app

RUN apt-get update && apt-get install -y gcc g++ libpq-dev && \
    pip install --no-cache-dir pandas numpy scipy scikit-learn sqlalchemy psycopg2-binary matplotlib
COPY screener_plot.py .
CMD ["python", "screener_plot.py"]
