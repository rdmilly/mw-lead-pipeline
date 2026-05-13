FROM python:3.12-slim
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN pip install --no-cache-dir fastapi uvicorn httpx beautifulsoup4 dnspython python-multipart psycopg2-binary python-whois

COPY app.py config_routes.py dashboard.html db.py deep_email_extractor.py email_discovery.py email_guesser.py enrich_full_routes.py enrich_routes.py enricher_v2.py enrichment_full.py enrichment_orchestrator.py extractor.py git_sync.py healthcheck.sh instantly_push.py leads_routes.py maps_scraper.py master_sync.py multi_source_scraper.py ops_routes.py pipeline_auto.py pipeline_email.py places_finder.py scorer.py scraper_worker.py website_intelligence.py yelp_bbb.py  ./

EXPOSE 8099
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8099"]
