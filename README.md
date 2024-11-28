# TaxiRides Project
The project structure was designed considering it can be run in a production setup. The pipeline is based on achieving the steps given below.
- Taking the data from https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page <strong>(task1.py)</strong>
- Writing a Python program that calculates the average trip length of all Yellow Taxis for a month. <strong>(task1.py)</strong>
- Extending this to a data pipeline that can ingest new data and calculates the 45 day rolling average trip length. <strong>(task2.py)</strong>
- Additionally, documenting how to scale the pipeline to a multiple of the data size that does not fit any more to one machine.

To be able to achieve all the steps except the last one, <strong>task1.py</strong> and <strong>task2.py</strong> must be run successively. 

The last step was documented at the end of this readme file.

## Setup
1. Clone the repository:
  ```
    git clone https://github.com/serkosi/TaxiRides.git 

    cd TaxiRides
  ```

2. Create a virtual environment (optional but recommended):
  ```
    python -m venv venv
    source venv/bin/activate    # Linux or macOS
    venv\Scripts\activate       # Windows
  ```
3. Install the required packages:
  ```
    pip install -r requirements.txt
  ```

4. Set up configuration:

A <strong>config.yaml</strong> file is located in the root directory of the project. This file includes the necessary configuration parameters and doesn't need to be modified.

5. You're now ready to run the scripts!

## Start
To start the tasks simply run the following commands from the parent TaxiRides directory:
  ```
python task1.py
python task2.py
  ```

## Assumptions / Notes
* The data is retrieved via an API endpoint and base url was generated with the dataset identifier: 4b4i-vvec belonging to <strong>2023 Yellow Taxi Trip Data</strong>. Relevant details were taken from the documents located in <strong>dicts-metadata</strong> folder.
* The initial data will be saved after running <strong>task1.py</strong> and will be updated with the ingested data after running <strong>task2.py</strong>.
    
    For the sake of fast data retrieval, only one month is taken into consideration as a starting point in <strong>task1.py</strong>. The following month was defined as a second date range for the <strong>task2.py</strong>.
* The term <strong>trip length</strong> of a taxi ride can refer to either the distance between the starting and finishing points or the time spent during the ride, depending on the context.

    In this project, trip length is considered as the time spent during the ride. That allowed keeping the data stored as small as possible without considering different variables than the pick-up and drop-off times.

## Testing
To test <strong>task1.py</strong> and <strong>task2.py</strong>, one can use the following commands from the parent TaxiRides directory:
  ```
python -m unittest tests.test_task1
python -m unittest tests.test_task2
  ```

## Scaling Pipeline to Multiple Data Size
Streaming Processing, Containerization and Orchestration or a Cloud-based Solutions can be useful to handle the pipeline to a larger data sizes that does not fit any more to one machine.

Implementing a data processing in real-time as the data arrives might be the most basic solution for the current pipeline. Modification of <strong>create_params function</strong> in <strong>util.py</strong> as below is a grouping approach. It would retrive the aimed averaging result, reduce the need for batch processing of large datasets.
```
    params = {
        '$select': 'AVG(trip_distance) as avg_distance, date_extract_y(tpep_pickup_datetime) as year, date_extract_m(tpep_pickup_datetime) as month',
        '$where': f"date_extract_y(tpep_pickup_datetime) = {year} AND date_extract_m(tpep_pickup_datetime) = {month}",
        '$group': 'date_extract_y(tpep_pickup_datetime), date_extract_m(tpep_pickup_datetime)',
    }
```
Using Docker to containerize the application and employing Kubernetes for orchestrating the deployment is also a good choice to scale the pipeline across a cluster.

Using Airflow, Astronomer, and a cloud solution such as BigQuery is another approach for scaling the pipeline. Programmatically scheduling workflows, defining the pipeline as a series of tasks (DAGs), triggering it based on certain events and storing it on a serverless data warehouse with complex query capabilities provide significant scaling advantages with large data.

Lastly, Dask might also be a good choice for scaling to a cluster of machines. Dask works in similar interfaces to other Python libraries such as NumPy and Pandas. Handling datasets larger than available RAM of local machine by processing data in chunks is promising for scaling purposes.

### Containerized Application and Kubernetes Orchestration
The process follows a sequential processing pattern using Kubernetes Jobs with init container to ensure proper data handling and verification:
1. Init container (`taxi-rides`): 
   - Primary container that executes the data processing tasks
   - Processes raw taxi data and generates a Parquet file (`taxi_trips.parquet`)
   - Must complete successfully before the data handler starts
2. Data handler container: 
   - Verifies the processed data
   - Ensures the Parquet file is readable and valid
   - Reports the number of processed rows
   - Fails explicitly if verification checks don't pass

#### Volume Configuration
The application uses two volume types:
- ConfigMap volume: Stores configuration settings
- PersistentVolumeClaim: Stores processed data and enables data sharing between containers

#### Known Limitations
- The data retrieval (temporary) pod has a 1-hour lifetime
- Manual cleanup of the retrieval pod is required after data copying

#### Execution Steps
Make sure to have Docker and kubectl installed and configured properly.
1. Create ConfigMap from the config.yaml:
```
kubectl create configmap taxi-rides-config --from-file=config.yaml
```
2. Create GitHub Container Registry secret (replace with your credentials):
```
kubectl create secret docker-registry ghcr-secret --docker-server=ghcr.io --docker-username=YOUR_GITHUB_USERNAME --docker-password=YOUR_GITHUB_PAT --docker-email=YOUR_EMAIL
```
3. Build and push Docker image:
```
docker build -t ghcr.io/serkosi/taxi-rides:latest .
docker push ghcr.io/serkosi/taxi-rides:latest
```
4. Create PVC (Persistent Volume Claim):
```
kubectl apply -f taxi-rides-pvc.yaml
```
5. Apply the job:
```
kubectl apply -f taxi-rides-job.yaml
```
6. Watch the processing status:
```
# View the logs of the main processing container
kubectl logs -l job-name=taxi-rides-job -c taxi-rides

# View the data handler logs
kubectl logs -l job-name=taxi-rides-job -c data-handler
```
7. After job completion, data can be retrieved using a temporary pod that mounts the same persistent volume:
```
# Create the temporary pod
kubectl apply -f data-retrieval-pod.yaml

# Wait for pod to be ready
kubectl wait --for=condition=Ready pod/data-retrieval-pod

# Copy the Parquet file locally
kubectl cp data-retrieval-pod:/app/data/taxi_trips.parquet ./data/taxi_trips.parquet
```
9. When finished, clean up all Kubernetes resources related to the project:
```
kubectl delete pod data-retrieval-pod
kubectl delete job taxi-rides-job
kubectl delete pvc taxi-rides-pvc
kubectl delete configmap taxi-rides-config
kubectl delete secret ghcr-secret
```
