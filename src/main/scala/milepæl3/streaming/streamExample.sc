import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .master("local[*]")
  .appName("HDFS")
  .getOrCreate()

import org.apache.spark.sql.SparkSession
import spark.implicits._

val streamIn = spark.readStream
  .format("kafka")
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
  .option("kafka.sasl.jaas.config", """org.apache.kafka.common.security.scram.ScramLoginModule required username="aneqi8m2" password="tiYqB_68T6l8OZU30p22LqTrXAsfEmCJ";""")
  .option("kafka.bootstrap.servers", "rocket-01.srvs.cloudkafka.com:9094,rocket-02.srvs.cloudkafka.com:9094,rocket-03.srvs.cloudkafka.com:9094")
  .option("subscribe", "aneqi8m2-news")
  .option("startingOffsets", "earliest")
  .load()

val waterMarked = streamIn.withWatermark("timestamp", "1 second")
val formattedDF = waterMarked.select($"timestamp", $"value".cast("string").alias("parsedValue"))


val query2 = formattedDF.writeStream.format("console").start()

print(query2.status)
query2.stop()

/*val cityPopDataset = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv("worldcitiespop.csv")

val populationPerGrid =
    cityPopDataset
    .filter($"Country" === "us")
    .select($"Latitude", $"Longitude", coalesce($"Population", lit(0)).as("Population"))
    .groupBy(
        $"Latitude".cast("int").as("Latitude"),
        $"Longitude".cast("int").as("Longitude"))
    .sum("Population")


val input = spark.readStream
    .format("kafka")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
    .option("kafka.sasl.jaas.config", """org.apache.kafka.common.security.scram.ScramLoginModule required username="wr7bj921" password="7tN943r1gh2NRGELd_lBG_NGBUUkWqw4";""")
    .option("kafka.bootstrap.servers", "rocket-01.srvs.cloudkafka.com:9094,rocket-02.srvs.cloudkafka.com:9094,rocket-03.srvs.cloudkafka.com:9094")
    .option("subscribe", "wr7bj921-accidents")
    .option("startingOffsets", "earliest")
    .load()

val watermarkedInput = input.withWatermark("timestamp", "1 second") // treg pga. at spørringen skal fullføre i forelesningen
    

import org.apache.spark.sql.types._
val messageSchema = StructType(Array(
    StructField("Start_Lat",DoubleType,true),
    StructField("Start_Lng",DoubleType,true)))

val ourWindows = window( $"timestamp", "7 days", "7 days", "5 days")

val formattedDf = watermarkedInput.select(
    $"timestamp", 
    from_json($"value".cast("string"), messageSchema).alias("parsedValue"))

val groupedDf = formattedDf.groupBy( 
    ourWindows,
    $"parsedValue.Start_Lat".cast("int") as "lat",
    $"parsedValue.Start_Lng".cast("int") as "lng" ).count()

val joinedData = groupedDf
    .join( populationPerGrid, $"lat" === $"Latitude" && $"lng" === $"Longitude", "leftouter" )

val output = 
    joinedData
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()
*/