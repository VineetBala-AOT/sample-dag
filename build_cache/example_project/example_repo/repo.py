from dagster import op, job, schedule, repository


@op
def hello():
    """
    An op definition. This example op outputs a single string.
    For more hints about writing Dagster ops, see our documentation overview on Ops:
    https://docs.dagster.io/concepts/ops-jobs-graphs/ops
    """
    return "Hello, Dagster!"


@job
def say_hello_job():
    """
    A job definition. This example job has a single op.
    For more hints on writing Dagster jobs, see our documentation overview on Jobs:
    https://docs.dagster.io/concepts/ops-jobs-graphs/jobs-graphs
    """
    hello()


@schedule(
    cron_schedule="*/2 * * * *",
    job=say_hello_job,
    execution_timezone="US/Central",
)
def say_hello_job_schedule(_context):
    return {}


@repository
def etl_project():
    return [say_hello_job, say_hello_job_schedule]