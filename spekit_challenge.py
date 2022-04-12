import pyspark
from pyspark.sql.functions import date_format
from pyspark.sql import Window
from pyspark.sql.types import IntegerType,BooleanType,DateType,FloatType
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql import types as t
import findspark
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import weekofyear
from pyspark.sql.functions import dayofweek


'''
1. Pickup date filter to make sure the data belong to the corresponding months
2. trip distance should be greater than 0
3. per mile amount should be less than 100 - to make is more reasonable. We can even reduce it further by doing further Analysis. There were a few records with total_amount of 6k and even 300k. I was able to filter those out by using this condition
4. payment_type should be either card or cash
5. fare amount should be greater than 0
spark-submit --packages mysql:mysql-connector-java:8.0.28 ./spekit_challenge.py
'''


#findspark.add_packages('mysql:mysql-connector-java-8.0.28')
sc = pyspark.SparkContext('local[*]')
spark = SparkSession.builder.config("spark.jars", "/usr/local/Cellar/apache-spark/3.2.1/libexec/jars/mysql-connector-java-8.0.28.tar").master("local").appName("NYC_data_challenge").getOrCreate()

df = spark.read.option("header",True).csv('file://///Users/Steffi/Downloads/spekit/yellow_tripdata_2019-11.csv')
zone_lookup_df = spark.read.option("header",True).csv('file://///Users/steffi/Downloads/taxi+_zone_lookup.csv')

df1 = df.withColumn('Pickup_Date', split(df['tpep_pickup_datetime'], ' ').getItem(0))
df2 = df1.withColumn("total_amount",df1.total_amount.cast(FloatType())) \
    .withColumn("Pickup_Date",df1.Pickup_Date.cast(DateType())) \
    .withColumn("payment_type",df1.payment_type.cast(IntegerType())) \
    .withColumn("trip_distance",df1.trip_distance.cast(FloatType())) \
    .withColumn("fare_amount",df1.fare_amount.cast(FloatType()))

df3 = df2.filter((f.col('Pickup_Date') >= lit("2019-11-01")) & (f.col('Pickup_Date') <= lit("2019-11-30")) ) \
    .filter(f.col('total_amount')>0).filter((f.col('payment_type')==1) | (f.col('payment_type')==2)) \
    .filter(f.col('trip_distance')>0).filter(f.col('fare_amount')>1).filter((f.col('fare_amount')/f.col('trip_distance'))<100)


w = Window.partitionBy('Pickup_Date')

max_df = df3.withColumn('max_amount', f.max('total_amount').over(w))\
    .where(f.col('total_amount') == f.col('max_amount'))\
    .drop('total_amount')\
    .sort(f.col('Pickup_Date'))

max_results_df = max_df.select(max_df["Pickup_Date"],max_df["PULocationID"],max_df["DOLocationID"],max_df["max_amount"]).withColumn("taxi_type",lit("yellow")).dropDuplicates(['Pickup_Date'])
# max_results_df = max_results_df.withColumnRenamed("Pickup_Date", "pickup_date") \
#        .withColumnRenamed("PULocationID", "pu_location_id") \
#        .withColumnRenamed("DOLocationID", "do_location_id")

min_df = df3.withColumn('min_amount', f.min('total_amount').over(w))\
    .where(f.col('total_amount') == f.col('min_amount'))\
    .drop('total_amount')\
    .sort(f.col('Pickup_Date'))

min_results_df = min_df.select(min_df["Pickup_Date"],min_df["PULocationID"],min_df["DOLocationID"],min_df["min_amount"]).withColumn("taxi_type",lit("yellow")).dropDuplicates(['Pickup_Date'])

results_df = df3.groupBy("Pickup_Date").agg(f.sum('total_amount')) \
        .withColumn("week_num",weekofyear(df3.Pickup_Date)) \
        .withColumn("week_day",dayofweek(df3.Pickup_Date)) \
        .sort(f.col('Pickup_Date')).withColumn("taxi_type",lit("yellow")) \
        .dropDuplicates(['Pickup_Date'])


zones_dict = zone_lookup_df.select('LocationID','Zone').rdd.collectAsMap()
dicts = sc.broadcast(zones_dict)

def zoneCols(x):
    return dicts.value[x]

zoneColsUdf = f.udf(zoneCols, t.StringType())


max_results_df = max_results_df.withColumn('Pickup_Zone', zoneColsUdf(f.col('PULocationID')))\
    .withColumn('Dropoff_Zone', zoneColsUdf(f.col('DOLocationID')))

max_results_df = max_results_df.withColumnRenamed("Pickup_Date", "pickup_date") \
       .withColumnRenamed("PULocationID", "pu_location_id") \
       .withColumnRenamed("DOLocationID", "do_location_id") \
       .withColumnRenamed("Pickup_Zone", "pickup_zone") \
       .withColumnRenamed("Dropoff_Zone", "dropoff_zone")
max_results_df.show(40)

max_results_df.write.format('jdbc').options(
      url='jdbc:mysql://<hostname>/<databasename>',
      driver='com.mysql.jdbc.Driver',
      dbtable='max_trips',
      user='<username>',
      password='<password>').mode('append').save()

min_results_df = min_results_df.withColumn('Pickup_Zone', zoneColsUdf(f.col('PULocationID')))\
    .withColumn('Dropoff_Zone', zoneColsUdf(f.col('DOLocationID')))

min_results_df = min_results_df.withColumnRenamed("Pickup_Date", "pickup_date") \
       .withColumnRenamed("PULocationID", "pu_location_id") \
       .withColumnRenamed("DOLocationID", "do_location_id") \
       .withColumnRenamed("Pickup_Zone", "pickup_zone") \
       .withColumnRenamed("Dropoff_Zone", "dropoff_zone")
min_results_df.show(40)

min_results_df.write.format('jdbc').options(
      url='jdbc:mysql://<hostname>/<databasename>',
      driver='com.mysql.jdbc.Driver',
      dbtable='min_trips',
      user='<username>',
      password='<password>').mode('append').save()


results_df = results_df.withColumnRenamed("Pickup_Date", "pickup_date") \
        .withColumnRenamed("sum(total_amount)", "revenue")
results_df.show(40)
results_df.write.format('jdbc').options(
      url='jdbc:mysql://<hostname>/<databasename>',
      driver='com.mysql.jdbc.Driver',
      dbtable='trip_revenue',
      user='<username>',
      password='<password>').mode('append').save()


