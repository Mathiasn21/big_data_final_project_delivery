package milepæl3.streaming

import java.time.LocalDateTime
import java.util.regex.Pattern

import org.apache.kafka.common.utils.Utils.sleep
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object ProduceMessages {
  private val schema = new StructType()
    .add("id", IntegerType, nullable = true)
    .add("title", StringType, nullable = true)
    .add("publication", StringType, nullable = true)
    .add("author", StringType, nullable = true)
    .add("date", StringType, nullable = true)
    .add("year", DoubleType, nullable = true)
    .add("month", DoubleType, nullable = true)
    .add("url", StringType, nullable = true)
    .add("content", StringType, nullable = true)
    .add("retrieved", StringType, nullable = true)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    spark = SparkSession.builder()
      .master("local[*]")
      .appName("Produce messages")
      .getOrCreate()

    val path = "D:\\data\\all_the_news"

    val streamIn = spark.readStream
      .option("maxFilesPerTrigger", 1)
      .text(path)

      streamIn.withColumn("parsed_values", extract(streamIn("value")))
      .writeStream
      .format("console")
      .option("truncate", value = false)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode(OutputMode.Update())
      .start().awaitTermination()

    /*read.writeStream.format("kafka")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
      .option("kafka.sasl.jaas.config", """org.apache.kafka.common.security.scram.ScramLoginModule required username="aneqi8m2" password="tiYqB_68T6l8OZU30p22LqTrXAsfEmCJ";""")
      .option("kafka.bootstrap.servers", "rocket-01.srvs.cloudkafka.com:9094,rocket-02.srvs.cloudkafka.com:9094,rocket-03.srvs.cloudkafka.com:9094")
      .option("topic", "aneqi8m2-Trump").option("checkpointLocation", "D:\\projects_git\\Semester5\\big_data\\produce_messages")
      .start()
      .awaitTermination()
      .load()*/
  }

  private def extract = udf((str: String) => {
    val regex = "^\\d+,(?<id>(\\\"(?:[^\\\"\\\\]|\\\\.)*\\\")|([^,]*))," +
      "(?<title>(\\\"(?:[^\\\"\\\\]|\\\\.)*\\\")|([^,]*))," +
      "(?<publication>(\\\"(?:[^\\\"\\\\]|\\\\.)*\\\")|([^,]+))," +
      "(?<author>(\\\"(?:[^\\\"\\\\]|\\\\.)*\\\")|([^,]*))," +
      "(?<date>(\\\"(?:[^\\\"\\\\]|\\\\.)*\\\")|([^,]*))," +
      "(?<year>(\\\"(?:[^\\\"\\\\]|\\\\.)*\\\")|([^,]*))," +
      "(?<month>(\\\"(?:[^\\\"\\\\]|\\\\.)*\\\")|([^,]*))," +
      "(?<url>(\\\"(?:[^\\\"\\\\]|\\\\.)*\\\")|([^,]*))," +
      "(?<content>(\".*\"))"
    //Had to alter the last regex(content) to avoid recurring group captures. Which caused fatal errors in spark

    sleep(500)
    val pattern = Pattern.compile(regex)
    val matcher = pattern.matcher(str)
    val res = ""

    if(matcher.find()) {
      val random = new scala.util.Random
      val timeRetrieved = LocalDateTime.now().minusSeconds(random.nextInt(3600)).toString

      val id = matcher.group("id")
      val title = matcher.group("title")
      val publication = matcher.group("publication")
      val author = matcher.group("author")
      val date = matcher.group("date")
      val year = matcher.group("year")
      val month = matcher.group("month")
      val content = matcher.group("content")

      val data = Seq(
        Row(id, title, publication,
          author, date, year, month,
          content, timeRetrieved)
      )
    }

    lit(res)
  })
}
