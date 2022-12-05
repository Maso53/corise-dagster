from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
)
def get_s3_data(context):
    key = context.op_config["s3_key"]
    s3 = context.resources.s3
    return [Stock.from_list(x) for x in s3.get_data(key)]

@op
def process_data(context, stock:list) -> Aggregation:
    max_item = Aggregation(date=datetime.now(), high=0.0)
    for stock in stock:
        if stock.high > max_item.high:
            max_item = Aggregation(date=stock.date, high=stock.high)
    return max_item

@op(
    required_resource_keys={'redis'},
)
def put_redis_data(context, aggregation: Aggregation):
    context.resources.redis.put_data(date(aggregation.date), aggregation.high)

@op(
    required_resource_keys={"s3"},
)
def put_s3_data(context, aggregation: Aggregation):
    context.resources.s3.put_data(date(aggregation.date), aggregation.high)


@graph
def week_2_pipeline():
    aggregation = process_data(get_s3_data())
    put_redis_data(aggregation)
    put_s3_data(aggregation)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

local_week_2_pipeline = week_2_pipeline.to_job(
    name="local_week_2_pipeline",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
    config=docker,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource
    }
)
