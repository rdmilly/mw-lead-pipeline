FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir httpx beautifulsoup4 playwright && \
    playwright install chromium && \
    playwright install-deps chromium

COPY . .

CMD ["python3", "runner.py"]
