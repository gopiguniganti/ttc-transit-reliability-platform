FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt

COPY poller.py replay.py .

RUN useradd -m poller && chown -R poller:poller /app
USER poller

CMD ["python3", "poller.py"]
