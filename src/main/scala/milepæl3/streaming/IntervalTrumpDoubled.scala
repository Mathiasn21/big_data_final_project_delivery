package milepæl3.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object IntervalTrumpDoubled{
  def main(args:Array[String]):Unit= {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
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
    val waterMarked = streamIn
      .withColumn("timestamp", unix_timestamp(streamIn("timestamp"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
      .withWatermark("timestamp", "1 second")

    val formattedDF = waterMarked
      .select($"timestamp", from_json($"value".cast("string"), getSchema).alias("data"))
      .select("timestamp", "data.*")

    val trumpFilteredDf = formattedDF
      .filter(lower($"author").contains("trump") || lower($"content").contains("trump"))

    //Group by and count number of occurrences inside the determined time interval
    val myWindow = window($"timestamp", "60 seconds", "60 seconds")
    val trumpWindowed = trumpFilteredDf.groupBy(myWindow).count()

    var lastIntervalCount = Long.MaxValue
    var lastTime:Row = null
    val staticWindow = Window.orderBy($"window")

    /*Utilizes a custom function for handling each batch and therein each row
    Utilized a memory sink as it does not really matter if data is lost in memory in this query.*/
    val query = trumpWindowed.writeStream
      .format("memory")
      .option("truncate", value = false)
      .option("checkpointLocation", "D:\\projects_git\\Semester5\\big_data\\test")
      .foreachBatch { (df: DataFrame, _: Long) => {
        //Use lag to remember previous values by 1 offset
        val newDf = df.withColumn("prev_count", lag($"count", 1, 0).over(staticWindow))
        newDf.foreach((row:Row) => {
          val count = row.getLong(1)
          val windowStruct = row.getStruct(0)
          var prev = row.getLong(2)
          //Handle special case where query is first ran or entering a new batch.
          if (lastTime != null && prev == 0 && (windowStruct.equals(lastTime) || windowStruct.getTimestamp(1).equals(lastTime.getTimestamp(0)))){
            prev = lastIntervalCount
          }
          //Assumed it was greater than or equal, as also accounts for greater rates of trump articles.
          //Which would make more sense from a business perspective
          if (count >= prev * 2) {
            print("Seeing a doubling of Trump!" + "\n")
            print( "Prev value: " + prev + " Current value: " + count + "\n\n\n")
          }
        })
        //Reverse sort, such that max value, last entry becomes first entry.
        val maxTime = newDf.sort($"window.end".desc).limit(1)
        val firstVal = maxTime.first()
        lastIntervalCount = firstVal.getLong(1)
        lastTime = firstVal.getStruct(0)

      }}.trigger(Trigger.ProcessingTime("61 seconds"))
      .outputMode(OutputMode.Update())
      .start()
    query.awaitTermination()
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
