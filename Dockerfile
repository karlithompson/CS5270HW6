FROM python:3.12-slim

COPY  consumer.py consumer.py
COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "consumer.py", "-r", "us-east-1", "-q", "cs5270-requests", "-dwt", "widgets"]

