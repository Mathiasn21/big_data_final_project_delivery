package milepæl3.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//Credentials(Uname, password, topic)

object StreamingExample3{
  var mapped:Map[String, String] = null

  def main(args:Array[String]):Unit= {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Kafka")
      .getOrCreate()

    import spark.implicits._

    val schema = StructType(
      StructField("_c0", StringType, nullable=false) ::
        StructField("_c1", StringType, nullable=false) ::
        StructField("_c2", StringType, nullable=false) ::
        StructField("_c3", StringType, nullable=false) :: Nil
    )
    //Utilizes a custom hdfs that was setup locally on a home server
    val path = "hdfs://10.0.0.95:9000/cmudict.dict"

    //Using a map to set options is sort off redundant as it only has one option though useful to note that possibility
    val df = spark.read.schema(schema).options(Map("delimiter"->" ")).csv(path)

    //Fill missing data with empty strings. Concat to fonet column and drop the rest.
    val df1 = df.na.fill("")
    val dictDf = df1.withColumn("fonet", concat_ws(" ", col("_c1"), col("_c2"), col("_c3"))).drop("_c1", "_c2", "_c3")

    //Map word to fonet -> Makes it faster to access the correct fonet
    mapped = dictDf.map(row => (row.getAs[String](0), row.getAs[String](1))).collect.toMap

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
    val formattedDF = waterMarked.select($"timestamp", from_json($"value".cast("string"), getSchema).alias("data"))
      .select("timestamp", "data.*")

    //Map fonets to title and remove certain characters
    val filteredDF = formattedDF
      .withColumn("CMUdict", mapFonetToWords(array_join(split($"title", "[,\\.\"\'\\?\\@\\s]"), ",")))
      .select($"timestamp", $"author", $"title", $"date", $"CMUdict")

    val query = filteredDF.writeStream
      .format("console")
      .option("truncate", value = false).start()
    query.awaitTermination()
  }

  private def mapFonetToWords = udf((str: String) => {
    var fonets = Seq[String]()
    str.split(",").foreach((word:String) => {
      try {
        val v = mapped(word.toLowerCase)
        fonets = fonets :+ v
      } catch {
        case e: Exception =>
          print("Word not found :( " + word, "\n\n\n\n", e)
          fonets = fonets :+ word
      }
    })
    fonets.mkString(" ")
  })

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