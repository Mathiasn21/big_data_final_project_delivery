package milepæl3.streaming

import java.time.LocalDateTime
import java.util.regex.Pattern

import org.apache.kafka.common.utils.Utils.sleep
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, udf}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.json4s._
import org.json4s.jackson.Json


object ProduceMessages {
  private val schema = new StructType()
    .add("id", StringType, nullable = true)
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
    val path = "D:\\data\\all_the_news"

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Produce messages")
      .getOrCreate()

    val streamIn = spark.readStream
      .option("maxFilesPerTrigger", 1)
      .text(path)

    val preparedStream = streamIn
      //First parse raw text from badly formatted csv file into json using a udf
      .withColumn("value", parseTextToJson(streamIn("value")))

      //Then extract id by converting it from json and grabbing the id, which is then cast to key as required by cloudkarafka
      .withColumn("key", from_json(col("value"), schema).getItem("id").cast(StringType))

    //And finally setup config for streaming to kafka
    preparedStream
      .writeStream
      .format("kafka")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
      .option("kafka.sasl.jaas.config", """org.apache.kafka.common.security.scram.ScramLoginModule required username="aneqi8m2" password="tiYqB_68T6l8OZU30p22LqTrXAsfEmCJ";""")
      .option("kafka.bootstrap.servers", "rocket-01.srvs.cloudkafka.com:9094,rocket-02.srvs.cloudkafka.com:9094,rocket-03.srvs.cloudkafka.com:9094")
      .option("topic", "aneqi8m2-news")
      .option("checkpointLocation", "D:\\projects_git\\Semester5\\big_data\\test")
      .start().awaitTermination()
  }

  private def parseTextToJson = udf(f = (str: String) => {
    val regex = "^\\d+,(?<id>(\\\"(?:[^\\\"\\\\]|\\\\.)*\\\")|([^,]*))," +
      "(?<title>(\\\"(?:[^\\\"\\\\]|\\\\.)*\\\")|([^,]*))," +
      "(?<publication>(\\\"(?:[^\\\"\\\\]|\\\\.)*\\\")|([^,]+))," +
      "(?<author>(\\\"(?:[^\\\"\\\\]|\\\\.)*\\\")|([^,]*))," +
      "(?<date>(\\\"(?:[^\\\"\\\\]|\\\\.)*\\\")|([^,]*))," +
      "(?<year>(\\\"(?:[^\\\"\\\\]|\\\\.)*\\\")|([^,]*))," +
      "(?<month>(\\\"(?:[^\\\"\\\\]|\\\\.)*\\\")|([^,]*))," +
      "(?<url>(\\\"(?:[^\\\"\\\\]|\\\\.)*\\\")|([^,]*))," +
      "(?<content>(\".*\"))$"
    //Had to alter the last regex(content) to avoid recurring group captures. Which caused fatal errors in spark

    val pattern = Pattern.compile(regex)
    val matcher = pattern.matcher(str)
    var res = ""

    //Find all groups
    if (matcher.find()) {
      try {
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

        //Identify data with headers
        val data = Map(
          "id" -> id,
          "title" -> title,
          "publication" -> publication,
          "author" -> author,
          "date" -> date,
          "year" -> year,
          "month" -> month,
          "content" -> content,
          "retrieved" -> timeRetrieved
        )
        //Convert content to json format
        res = Json(DefaultFormats).write(data)
      } catch {
        case _: Exception => print("Unparsable: ", str)
      }
    }
    res
  })
}
