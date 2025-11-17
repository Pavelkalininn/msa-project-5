FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY job_config.py .
RUN mkdir -p /app/output
CMD ["python", "job_config.py"]