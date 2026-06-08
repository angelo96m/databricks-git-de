import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#Input data paths
input_green = "/Workspace/Repos/angelo.montesarchio@agilelab.it/databricks-git-de/dataset/green_tripdata_2026-01.parquet"
input_yellow = "/Workspace/Repos/angelo.montesarchio@agilelab.it/databricks-git-de/dataset/yellow_tripdata_2026-01.parquet"
taxi_zone = "/Workspace/Repos/angelo.montesarchio@agilelab.it/databricks-git-de/dataset/taxi_zone_lookup.csv"

#Output table name
output_table = "trips_result"

#Create Spark session
spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

#Read data
df_green = spark.read.parquet(input_green)
df_yellow = spark.read.parquet(input_yellow) 
df_zone = spark.read.csv(taxi_zone, header=True)

#Rename columns for consistency
df_green = df_green \
    .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

df_yellow = df_yellow \
    .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

#Define common columns for both datasets
common_colums = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'store_and_fwd_flag',
    'RatecodeID',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'payment_type',
    'congestion_surcharge'
]


df_green_sel = df_green.select(common_colums) \
    .withColumn('service_type', F.lit('green'))


df_yellow_sel = df_yellow.select(common_colums) \
    .withColumn('service_type', F.lit('yellow'))

#Combine both datasets
df_trips_data = df_green_sel.unionAll(df_yellow_sel)

#Define temporary table for SQL queries
df_trips_data.createOrReplaceTempView('trips_data')


df_result = spark.sql("""
SELECT 
    -- Revenue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")

df_result.write.mode("overwrite").saveAsTable(output_table)

print("Finished successfully!!!")