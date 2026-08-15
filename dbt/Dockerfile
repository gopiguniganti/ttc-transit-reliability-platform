FROM python:3.12-slim

WORKDIR /dbt
COPY requirements.txt .
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt

COPY . .
ENV DBT_PROFILES_DIR=/dbt

CMD ["dbt", "build"]
