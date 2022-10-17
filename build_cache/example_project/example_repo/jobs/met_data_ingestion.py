from dagster import job
from ops.extract_load import hello

@job
def met_data_ingestion():
    hello()
