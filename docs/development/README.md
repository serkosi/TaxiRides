# Development Guide

This guide covers local setup, running the collection pipeline, and its tests. For the pipeline design and scaling notes, see the [Pipeline Guide](../pipeline/README.md). For demand trends and machine-learning forecasts, see the [project overview](../../README.md).

## Setup

From the repository root:

```bash
git clone https://github.com/serkosi/TaxiRides.git
cd TaxiRides
python -m venv .venv
source .venv/bin/activate  # Linux or macOS
# Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

The API endpoint and date ranges are in `config.yaml`; adjust those values if you want to collect a different range.

## Run the Pipeline

Run the two processing tasks in order:

```bash
python task1.py
python task2.py
```

The scripts write `data/taxi_trips.parquet`, which is also the input expected by the forecasting notebook. Open `notebooks/taxi_demand_forecasting.ipynb` in Jupyter or VS Code after the parquet file has been generated.

## Tests

Run the pipeline tests from the repository root:

```bash
python -m unittest tests.test_task1
python -m unittest tests.test_task2
```
