# Data Ingestion

The structure in this folder is designed to facilitate the ingestion of EVTX files into Elasticsearch.

## Instructions

1. Place your EVTX files in the `data/input` directory.

2. Run Docker Compose to start the services and ingest the data:

   ```bash
   docker compose up -d --build
   ```

3. Monitor the logs of the ingestor service to track progress:

   ```bash
   docker compose logs -f ingestor
   ```

4. Access Kibana to visualize the ingested data:
   - Open your web browser and navigate to [http://localhost:5601](http://localhost:5601).
   - Use the Kibana interface to explore the ingested EVTX data.
