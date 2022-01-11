from datetime import date

import pyspark
from pyspark.sql import SparkSession, functions as f, types as t

spark = SparkSession.builder.getOrCreate()

data = [
    (date(year=2021, month=1, day=31),),
    (date(year=2021, month=1, day=30),),
    (date(year=2021, month=1, day=28),),
    (date(year=2021, month=2, day=28),),
]

schema = t.StructType([
    t.StructField('date', t.DateType(), nullable=False)
])

df = spark.createDataFrame(data=data, schema=schema)
df = df.withColumn('add_1_month', f.add_months('date', 1))

df.coalesce(1).write.csv(
    'result_spark{}'.format(pyspark.__version__),
    mode='overwrite',
    header=True
)
