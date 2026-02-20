FROM python:3.11-slim

WORKDIR /app

RUN useradd --no-create-home --shell /bin/false exporter

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY asterisk_exporter/ ./asterisk_exporter/

USER exporter

EXPOSE 9100

ENTRYPOINT ["python", "-m", "asterisk_exporter.exporter"]
