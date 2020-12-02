package milepæl3.streaming

import java.util.regex.Pattern

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._
//Credentials(Uname, password, topic)

object StreamingExercise4{
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

    val formattedDF = streamIn
      .select($"timestamp", from_json($"value".cast("string"), getSchema).alias("data"))
      .select("timestamp", "data.*")

    val waterMarked = formattedDF
      .withColumn("retrieved", unix_timestamp(formattedDF("retrieved"), "yyyy-MM-dd HH:mm:ss.SSSSSS").cast("timestamp"))
      .withWatermark("retrieved", "120 seconds")

    val filteredDf = waterMarked.filter(
      lower($"author").contains("trump") ||
        lower($"author").contains("clinton") ||
        lower($"author").contains("biden") ||
        lower($"content").contains("trump") ||
        lower($"content").contains("clinton") ||
        lower($"content").contains("biden")
    )

    val countedDf = filteredDf
      .withColumn("words", extractAll(lower($"title"), lit("(clinton|biden|trump)")))
      .filter($"words".notEqual(""))
      .withColumn("words", explode(split($"words", ",")))

    val myWindow = window($"retrieved", "45 seconds", "45 seconds")
    val windowed = countedDf.groupBy(myWindow, $"words").count()

    windowed
      .writeStream
      .format("console")
      .option("truncate", value = false)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .outputMode(OutputMode.Update())
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

  private def extractAll = udf((str: String, exp: String) => {
    val pattern = Pattern.compile(exp)
    val matcher = pattern.matcher(str)
    var res = Seq[String]()
    while (matcher.find) {
      res = res :+ matcher.group(0)
    }
    res.mkString(",")
  })
}
