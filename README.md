# TaxiRides Project
The project structure was designed considering it can be run in a production setup. The pipeline is based on achieving the steps given below.
- Take the data from https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page <strong>(task1.py)</strong>
- Please write a Python program that calculates the average trip length of all Yellow Taxis for a month. <strong>(task1.py)</strong>
- Extend this to a data pipeline that can ingest new data and calculates the 45 day rolling average trip length. <strong>(task2.py)</strong>
- Additionally, please document how you would scale your pipeline to a multiple of the data size that does not fit any more to one machine.

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

    In this project, trip length is considered as the time spent during the ride. That allowed me to keep the data stored as small as possible without considering different variables than the pick-up and drop-off times.

## Testing
To test <strong>task1.py</strong> and <strong>task2.py</strong>, one can use the following commands from the parent TaxiRides directory:
  ```
python -m unittest tests.test_task1
python -m unittest tests.test_task2
  ```

## Scaling Pipeline to Multiple Data Size
Streaming Processing, Containerization and Orchestration or a Cloud-based Solutions can be useful to handle the pipeline to a larger data sizes that does not fit any more to one machine.

Implementing a data processing in real-time as the data arrives might be the most basic solution for my current pipeline. Modification of <strong>create_params function</strong> in <strong>util.py</strong> as below is a grouping approach. It would retrive the aimed averaging result, reduce the need for batch processing of large datasets.
```
    params = {
        '$select': 'AVG(trip_distance) as avg_distance, date_extract_y(tpep_pickup_datetime) as year, date_extract_m(tpep_pickup_datetime) as month',
        '$where': f"date_extract_y(tpep_pickup_datetime) = {year} AND date_extract_m(tpep_pickup_datetime) = {month}",
        '$group': 'date_extract_y(tpep_pickup_datetime), date_extract_m(tpep_pickup_datetime)',
    }
```
Using Docker to containerize the application and employing Kubernetes for orchestrating the deployment is also a good choice to scale the pipeline across a cluster.

Using Airflow, Astronomer, and a cloud solution such as BigQuery is another approach for scaling my pipeline. Programmatically scheduling workflows, defining the pipeline as a series of tasks (DAGs), triggering it based on certain events and storing it on a serverless data warehouse with complex query capabilities provide significant scaling advantages with large data.

Lastly, Dask might also be a good choice for scaling to a cluster of machines, although I've just started to gain familiarity with it. Dask works in similar interfaces to other Python libraries such as NumPy and Pandas. Handling datasets larger than available RAM of local machine by processing data in chunks is promising for scaling purposes.
