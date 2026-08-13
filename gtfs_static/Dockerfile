FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt

COPY load_dimensions.py .

CMD ["python3", "load_dimensions.py"]
