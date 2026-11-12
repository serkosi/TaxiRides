# Taxi Data Pipeline

This guide covers the original data collection and processing tasks, their assumptions, and options for scaling the pipeline. For environment setup, running commands, and tests, see the [Development Guide](../development/README.md). For the daily-demand analysis, see the [project overview](../../README.md) and [forecasting notebook](../../notebooks/taxi_demand_forecasting.ipynb).

## Data Source

The pipeline retrieves 2023 NYC Yellow Taxi trip records from the TLC dataset endpoint configured in `config.yaml`. The project initially collects one month of data and then ingests the following month. The raw parquet file contains pickup and drop-off timestamps.

## Processing Tasks

Run `task1.py` followed by `task2.py` from the repository root:

1. `task1.py` fetches the initial date range, saves `data/taxi_trips.parquet`, and calculates trip durations and daily summaries.
2. `task2.py` fetches the next configured date range, appends those records to the local parquet data, and calculates the 45-day rolling average of daily total trip time.

The pipeline defines trip length as elapsed time between pickup and drop-off, rather than distance. The forecasting notebook uses the same collected records for a different target: daily pickup counts.

## Assumptions and Limitations

- The API base URL uses the NYC TLC Yellow Taxi dataset identifier configured in `config.yaml`.
- The initial and follow-up date ranges are configured separately in `config.yaml`.
- Trip duration is derived from pickup and drop-off timestamps; other trip attributes are not part of the stored data.
- The forecasting dataset covers January and February 2023 only. It supports a short exploratory forecast but not reliable annual seasonality conclusions.

## Scaling the Pipeline

For data volumes beyond a single machine, possible approaches include:

- **Aggregate at the source:** Where supported by the API, use grouped queries to retrieve daily or monthly summaries rather than individual records.
- **Distributed processing:** Dask can process data in partitions and can be extended to a cluster.
- **Orchestration and storage:** Airflow or Astronomer can schedule pipeline tasks, while a data warehouse such as BigQuery can store and query larger datasets.
- **Containerized workloads:** Docker and Kubernetes can package, schedule, and scale processing jobs.

The existing Kubernetes configuration runs the data-processing job and a data-verification handler with shared persistent storage. The temporary data-retrieval pod has a one-hour lifetime and needs manual cleanup after copying the parquet file.

### Kubernetes workflow

The following commands assume Docker and `kubectl` are installed and configured. Replace the registry credentials with your own values.

1. Create the ConfigMap and registry secret:

   ```bash
   kubectl create configmap taxi-rides-config --from-file=config.yaml
   kubectl create secret docker-registry ghcr-secret \
     --docker-server=ghcr.io \
     --docker-username=YOUR_GITHUB_USERNAME \
     --docker-password=YOUR_GITHUB_TOKEN \
     --docker-email=YOUR_EMAIL
   ```

2. Build and push the image, then create the persistent volume and job:

   ```bash
   docker build -t ghcr.io/serkosi/taxi-rides:latest .
   docker push ghcr.io/serkosi/taxi-rides:latest
   kubectl apply -f taxi-rides-pvc.yaml
   kubectl apply -f taxi-rides-job.yaml
   ```

3. Check both container logs:

   ```bash
   kubectl logs -l job-name=taxi-rides-job -c taxi-rides
   kubectl logs -l job-name=taxi-rides-job -c data-handler
   ```

4. After the job completes, retrieve the parquet file:

   ```bash
   kubectl apply -f data-retrieval-pod.yaml
   kubectl wait --for=condition=Ready pod/data-retrieval-pod
   kubectl cp data-retrieval-pod:/app/data/taxi_trips.parquet ./data/taxi_trips.parquet
   ```

5. Remove the resources when finished:

   ```bash
   kubectl delete pod data-retrieval-pod
   kubectl delete job taxi-rides-job
   kubectl delete pvc taxi-rides-pvc
   kubectl delete configmap taxi-rides-config
   kubectl delete secret ghcr-secret
   ```
