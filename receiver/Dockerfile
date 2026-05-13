FROM python:3.12-slim
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN pip install --no-cache-dir fastapi uvicorn httpx beautifulsoup4 dnspython python-multipart psycopg2-binary python-whois

COPY app.py db.py master_sync.py import_recovered.py enrichment_orchestrator.py scorer.py instantly_push.py places_finder.py pipeline_auto.py yelp_bbb.py extractor.py leads_routes.py config_routes.py enrich_routes.py scraper_worker.py enricher_v2.py enrichment_full.py enrich_full_routes.py email_discovery.py email_guesser.py maps_scraper.py deep_email_extractor.py multi_source_scraper.py pipeline_email.py website_intelligence.py ops_routes.py dashboard.html ./

EXPOSE 8099
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8099"]
