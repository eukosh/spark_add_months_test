from datetime import date

import pyspark
from pyspark.sql import SparkSession, functions as f, types as t
from functools import singledispatch
from pyspark.sql import Column


# @singledispatch
# def add_months(startDate: Column, numMonths: int):
#     use_me(startDate, f.lit(numMonths))
#
#
# @add_months.register
# def use_me(startDate: Column, numMonths: Column):
#     add_months_spark = f.add_months(startDate, numMonths)
#     startDateIsLastDay = f.last_day(startDate) == startDate
#
#     return f.when(startDateIsLastDay, f.last_day(add_months_spark)).otherwise(add_months_spark)


spark = SparkSession.builder.getOrCreate()

data = [
    (date(year=2021, month=1, day=31),),
    (date(year=2021, month=1, day=30),),
    (date(year=2021, month=1, day=28),),
    (date(year=2021, month=2, day=28),),
    (date(year=2020, month=2, day=28),),
]

schema = t.StructType([
    t.StructField('date', t.DateType(), nullable=False)
])

df = spark.createDataFrame(data=data, schema=schema)
df = df.withColumn('add_1_month', f.add_months('date', 1))

# df = df.withColumn('my_add_month', add_months(f.col('date'), 1))

df.coalesce(1).write.csv(
    'result_spark{}'.format(pyspark.__version__),
    mode='overwrite',
    header=True
)
