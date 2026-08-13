FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt

COPY load_weather.py load_road_restrictions.py .

CMD ["python3", "load_weather.py"]
