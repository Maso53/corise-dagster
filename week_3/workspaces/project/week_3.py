from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    SkipReason,
    String,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
)
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"}
)
def get_s3_data(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    return [Stock.from_list(stock) for stock in context.resources.s3.get_data(s3_key)]


@op
def process_data(context, stocks: List[Stock]) -> Aggregation:
    highest_stock = max(stocks, key=lambda stock:stock.high)
    return Aggregation(date=highest_stock.date, high=highest_stock.high)


@op (
    required_resource_keys={"redis"}
)
def put_redis_data(context, stock_aggregation: Aggregation):
    context.resources.redis.put_data(name=str(stock_aggregation.date), value=str(stock_aggregation.high))


@op (
    required_resource_keys={"s3"}
)
def put_s3_data(context, stock_aggregation: Aggregation):
    context.resources.s3.put_data(key=stock_aggregation.date, data=stock_aggregation)


@graph
def week_3_pipeline():
    stocks = get_s3_data()
    processed = process_data(stocks)
    put_redis_data(processed)
    put_s3_data(processed)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


def docker_config():
    pass


week_3_pipeline_local = week_3_pipeline.to_job(
    name="week_3_pipeline_local",
)

week_3_pipeline_docker = week_3_pipeline.to_job(
    name="week_3_pipeline_docker",
)


week_3_schedule_local = None


@schedule
def week_3_schedule_docker():
    pass


@sensor
def week_3_sensor_docker():
    pass
