FROM python:3.12-slim

WORKDIR /app

COPY MoteurCorrelation/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY MoteurCorrelation/ .

CMD ["python", "subscriber.py"]