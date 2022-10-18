from dagster import op, job, schedule, repository
from typing import Dict, List 
import psycopg2
import psycopg2.extras as p
import os
from contextlib import contextmanager
from dataclasses import dataclass


@dataclass
class DBConnection:
    db: str
    user: str
    password: str
    host: str
    port: int = 5432


class WarehouseConnection:
    def __init__(self, db_conn: DBConnection):
        self.conn_url = (
            f"postgresql://{db_conn.user}:{db_conn.password}@"
            f"{db_conn.host}:{db_conn.port}/{db_conn.db}"
        )

    @contextmanager
    def managed_cursor(self, cursor_factory=None):
        self.conn = psycopg2.connect(self.conn_url)
        self.conn.autocommit = True
        self.curr = self.conn.cursor(cursor_factory=cursor_factory)
        try:
            yield self.curr
        finally:
            self.curr.close()
            self.conn.close()


def get_met_analytics_db_creds() -> DBConnection:
    return DBConnection(
        user=os.getenv("MET_ANALYTICS_DB_USER", ""),
        password=os.getenv("MET_ANALYTICS_DB_PASSWORD", ""),
        db=os.getenv("MET_ANALYTICS_DB_DB", ""),
        host=os.getenv("MET_ANALYTICS_DB_HOST", ""),
        port=int(os.getenv("MET_ANALYTICS_DB_PORT", 54334)),
    )


def get_met_db_creds() -> DBConnection:
    return DBConnection(
        user=os.getenv("MET_DB_USER", ""),
        password=os.getenv("MET_DB_PASSWORD", ""),
        db=os.getenv("MET_DB_DB", ""),
        host=os.getenv("MET_DB_HOST", ""),
        port=int(os.getenv("MET_DB_PORT", 5432)),
    )

@op
def extract_user_data() -> List[Dict[str, str]]:
    with WarehouseConnection(get_met_db_creds()).managed_cursor() as curr:
        curr.execute(
            '''
            select 
                'Y' as is_active,
                email_id as name,
                1 as runcycle_id
            from public.user where id = 8
            '''
        )
        user_data = curr.fetchall()
    return [
        {
            "is_active": str(d[0]),
            "name": str(d[1]),
            "runcycle_id": str(d[2]),
        }
        for d in user_data
    ]

@op
def load_user_data(user_data_to_load: List[Dict[str, str]]):
    ins_qry = """
    INSERT INTO public.user_details(
        is_active,
        name,
        runcycle_id
    )
    VALUES (
        %(is_active)s,
        %(name)s,
        %(runcycle_id)s
    )
    """
    with WarehouseConnection(get_met_analytics_db_creds()).managed_cursor() as curr:
        p.execute_batch(curr, ins_qry, user_data_to_load)


@job
def met_data_ingestion():
    load_user_data(extract_user_data())


@schedule(
    cron_schedule="*/2 * * * *",
    job=met_data_ingestion,
    execution_timezone="US/Central",
)
def met_data_ingestion_schedule(_context):
    return {}


@repository
def etl_project():
    return [met_data_ingestion, met_data_ingestion_schedule]