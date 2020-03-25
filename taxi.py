from pyspark.sql import SparkSession, window
from pyspark.sql.functions import avg
from pyspark.sql.types import *


def parse_data_from_kafka_message(sdf,scheme):
    from  pyspark.sql.functions import split
    assert sdf.isStreaming == True,"DataFrame doesn't receive streaming data"
    col =split(sdf['value'],",")
    for idx,field in enumerate(scheme):
        sdf=sdf.withColumn(field.name,col.getItem(idx).cast(field.dataType))
    return sdf.select([field.name for field in scheme])


if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("Spark Structured Streaming for taxi ride info")\
        .getOrCreate()

    rides = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers","localhost:9092")\
        .option("subscribe","taxirides")\
        .option("startingOffsets","latest")\
        .load()\
        .selectExpr("CAST(value AS STRING)")

    fares = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers","localhost:9092")\
        .option("subscribe","taxirides")\
        .option("startingOffsets","lastest")\
        .load()\
        .selectExpr("CAST(value AS STRING)")

    ridesSchema = StructType([\
        StructField("rideId",LongType()),\
        StructField("isStart",StringType()),\
        StructField("endTime",TimestampType()),\
        StructField("startTime",TimestampType()),\
        StructField("startLon",FloatType()),\
        StructField("startLat",FloatType()),\
        StructField("endLon",FloatType()),\
        StructField("endLat",FloatType()),\
        StructField("passengerCnt",ShortType()),\
        StructField("taxiId",LongType()),\
        StructField("driverId",LongType())])

    faresSchema = StructType([\
        StructField("rideId",LongType()),\
        StructField("taxiId",LongType()),\
        StructField("driverId",LongType()),\
        StructField("startTime",TimestampType()),\
        StructField("paymentType",StringType()),\
        StructField("tip",FloatType()),\
        StructField("tolls",FloatType()),\
        StructField("totalFare",FloatType())])

    rides= parse_data_from_kafka_message(rides,ridesSchema)
    fares = parse_data_from_kafka_message(fares,faresSchema)

    #找到紐約市內小費較高的地點
    MIN_LON,MAX_LON,MIN_LAT,MAX_LAT = -73.7,-74.05,41.0,40.5
    rides = rides.filter(\
        rides["startLon"].between(MIN_LON,MAX_LON)&\
        rides["startLat"].between(MIN_LAT,MAX_LAT)&\
        rides["endLon"].between(MIN_LON,MAX_LON)&\
        rides["endLat"].between(MIN_LAT,MAX_LAT))
    rides= rides.filter(rides["isStart"]=="END")

    #水印
    faresWithWatermark = fares\
        .selectExpr("rideId as rideId_fares","startTime","totalFare","tip")\
        .withWatermark("startTime","30 minutes")

    ridesWithWatermark = rides\
        .selectExpr("rideId","endTime","driverId","taxiId","startLon","startLat","endLon","endLat")\
        .withWatermark("endTime","30 minutes")

    joinDf = faresWithWatermark\
        .join(ridesWithWatermark,\
              expr("""\
              rideId_fares = rideId AND\
              endTime > startTime AND\
              endTime <= startTime + interval 2 hours\
              """))

    tips = joinDf\
        .groupBy(\
            window("endTime","30 minutes","10 minutes"),\
            "area")\
        .agg(avg("tip"))\
        .writeStream\
        .outputMode("append")\
        .format("console")\
        .option("truncate",False)\
        .start()\
        .awaitTermination()


