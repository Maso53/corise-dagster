import csv
from datetime import datetime
from typing import Iterator, List

from dagster import In, Nothing, Out, String, job, op, usable_as_dagster_type
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[List]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(config_schema={"s3_key": String})
def get_s3_data(context) -> List:
    stock = context.op_config["s3_key"]
    return list(csv_helper(stock))
    print(get_s3_data)
    
@op
def process_data(context, stock:list) -> Aggregation:
    max_high = max([x.high for x in stock])
    high_stock = [x for x in stock if x.high == max_high]
    high_stock = high_stock[0]
    agg = Aggregation(date=high_stock.date, high=high_stock.high)
    return agg

@op
def put_redis_data(context, Aggregation):
    pass

@job
def week_1_pipeline():
    stocks = get_s3_data()
    agg = process_data(stocks)
    put_redis_data(agg)
