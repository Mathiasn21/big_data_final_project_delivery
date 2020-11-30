package milepæl3.streaming

import java.util.regex.Pattern

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._

object ProduceMessages {
  //Credentials(Uname, password, topic)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val path = "D:\\data\\all_the_news"

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Produce messages")
      .getOrCreate()
    val regex = "^\\d+,(?P<id>(\"(?:[^\"\\]|\\.)*\")|([^,]*)),(?P<title>(\"(?:[^\"\\]|\\.)*\")|([^,]*)),(?P<publication>(\"(?:[^\"\\]|\\.)*\")|([^,]+)),(?P<author>(\"(?:[^\"\\]|\\.)*\")|([^,]*)),(?P<date>(\"(?:[^\"\\]|\\.)*\")|([^,]*)),(?P<year>(\"(?:[^\"\\]|\\.)*\")|([^,]*)),(?P<month>(\"(?:[^\"\\]|\\.)*\")|([^,]*)),(?P<url>(\"(?:[^\"\\]|\\.)*\")|([^,]*)),(?P<content>(\"(?:[^\"\\]|\\.)*\")|([^,]*))$"

    val schema = new StructType()
      .add("id", StringType, nullable = true)
      .add("title", StringType, nullable = true)
      .add("publication", StringType, nullable = true)
      .add("author", StringType, nullable = true)
      .add("date", StringType, nullable = true)
      .add("year", StringType, nullable = true)
      .add("month", StringType, nullable = true)
      .add("url", StringType, nullable = true)
      .add("content", StringType, nullable = true)
      .add("ZipCodeType", StringType, nullable = true)

    val streamIn = spark.readStream
      .option("maxFilesPerTrigger", 1)
      .textFile(path)

      .writeStream
      .format("console")
      .option("truncate", value = false)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode(OutputMode.Update())


      streamIn.start().awaitTermination()

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

  private def extract = udf((str: String, regex: String) => {
    val pattern = Pattern.compile(regex)
    val matcher = pattern.matcher(str)
    var res = Seq[String]()
    while (matcher.find) {
      res = res :+ matcher.group(0)
    }
    res.mkString(",")
  })
}
