package milepæl3.streaming
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json, lower, struct, to_json}
import org.apache.spark.sql.types._

object StreamingExercise3{
  def main(args:Array[String]):Unit= {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Kafka")
      .getOrCreate()

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

    //Watermark the stream and cast timestamp to type timestamp
    val waterMarked = streamIn.withWatermark("timestamp", "1 second")
    val formattedDF = waterMarked
      .select($"timestamp", from_json($"value".cast("string"), getSchema).alias("data"))
      .select("timestamp", "data.*")

    val filteredDf = formattedDF.filter(
      lower($"author").contains("trump") ||
      lower($"author").contains("biden") ||
      lower($"content").contains("trump") ||
      lower($"content").contains("biden")
    )

    val trumpFilteredDf = filteredDf.filter(
      lower($"author").contains("trump") ||
        lower($"content").contains("trump")
    )

    val bidenFilteredDf = filteredDf.filter(
      lower($"author").contains("biden") ||
        lower($"content").contains("biden")
    )

    bidenFilteredDf.select($"id".cast(StringType).as("key"), to_json(struct( $"author", $"id", $"content")).as("value"))
      .writeStream.format("kafka")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
      .option("kafka.sasl.jaas.config", """org.apache.kafka.common.security.scram.ScramLoginModule required username="aneqi8m2" password="tiYqB_68T6l8OZU30p22LqTrXAsfEmCJ";""")
      .option("kafka.bootstrap.servers", "rocket-01.srvs.cloudkafka.com:9094,rocket-02.srvs.cloudkafka.com:9094,rocket-03.srvs.cloudkafka.com:9094")
      .option("topic", "aneqi8m2-Biden").option("checkpointLocation", "D:\\projects_git\\Semester5\\big_data\\test2")
      .start()

    trumpFilteredDf.select($"id".cast(StringType).as("key"), to_json(struct( $"author", $"id", $"content")).as("value"))
      .writeStream.format("kafka")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
      .option("kafka.sasl.jaas.config", """org.apache.kafka.common.security.scram.ScramLoginModule required username="aneqi8m2" password="tiYqB_68T6l8OZU30p22LqTrXAsfEmCJ";""")
      .option("kafka.bootstrap.servers", "rocket-01.srvs.cloudkafka.com:9094,rocket-02.srvs.cloudkafka.com:9094,rocket-03.srvs.cloudkafka.com:9094")
      .option("topic", "aneqi8m2-Trump").option("checkpointLocation", "D:\\projects_git\\Semester5\\big_data\\test")
      .start()
      .awaitTermination()
  }

  private def getSchema:StructType={
    StructType(
      StructField("author", StringType, nullable = true) ::
      StructField("content", StringType, nullable = true) ::
      StructField("date", StringType, nullable = true) ::
      StructField("id", IntegerType, nullable = true) ::
      StructField("month", DoubleType, nullable = true) ::
      StructField("publication", StringType, nullable = true) ::
      StructField("retrieved", StringType, nullable = true) ::
      StructField("title", StringType, nullable = true) ::
      StructField("url", StringType, nullable = true) ::
      StructField("year", DoubleType, nullable = true) :: Nil
    )
  }
}
