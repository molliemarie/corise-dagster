import csv
from datetime import datetime
from typing import List

from dagster import In, Nothing, Out, job, op, usable_as_dagster_type
from pydantic import BaseModel
from operator import attrgetter


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: list):
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


@op(
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    output = list()
    with open(context.op_config["s3_key"]) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            stock = Stock.from_list(row)
            output.append(stock)
    return output

## Output is list of stocks

# This op will require the output of the get_s3_data (which will be a list of Stock). The output of the process_data will be our custom type Aggregation. 
@op(
    ins={"stock_list": In(dagster_type=List[Stock])},
    out={"max_stock": Out(dagster_type=Aggregation)},
    tags={},
    description="From a list of stocks, return an aggregation with the highest stock value",
)
def process_data(stock_list: List[Stock]):
    max_stock: Stock = max(stock_list, key=attrgetter("high"))
    return Aggregation(date=max_stock.date, high=max_stock.high)


@op(
    ins={"max_stock": In(dagster_type=Aggregation)},
    tags={"kind": "redis"},
    description="Upload max stock aggregation to redis",

)
def put_redis_data(max_stock):
    pass


@job
def week_1_pipeline():
    put_redis_data(process_data(get_s3_data()))
