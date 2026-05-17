FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY seed_orders.py .

ENTRYPOINT ["python", "-u", "seed_orders.py"]
