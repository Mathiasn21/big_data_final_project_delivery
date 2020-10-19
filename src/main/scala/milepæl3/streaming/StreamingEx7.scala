import java.util.regex.Pattern

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._

import scala.reflect.internal.util.TableDef.Column
//Credentials(Uname, password, topic)

object StreamingEx7{
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
    val df = spark.read.schema(schema).options(Map("delimiter"->" ")).csv("/home/mathias/data/cmudict.dict")

    val df1 = df.na.fill("")
    import  org.apache.spark.sql.functions._
    import org.apache.spark.sql.functions.col

    val dictDf = df1.withColumn("fonet", concat_ws(" ", col("_c1"), col("_c2"), col("_c3"))).drop("_c1", "_c2", "_c3")

    val mapped = dictDf.map(r => (r.getAs[String](0), r.getAs[String](1))).collect.toMap

    def myFunction = udf((str: Array[String]) => {
      var thing = Seq[String]()
      str.foreach((word:String) => {
        try {
          val v = mapped(word)
          thing = thing :+ v
        } catch {
          case e: Exception =>
            print("Word not found :( " + word)
            thing = thing :+ word
        }
      })
      thing.mkString(" ")
    })

    def getSchema:StructType={
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
    val filteredDF = formattedDF
      .withColumn("Fonetisk oversatt", myFunction(split($"title", "[!._,'’@?“”\"//\\$\\(\\)\\|\\:-\\s]"))
    )

    val query = filteredDF.writeStream.format("console").option("truncate", value = false).start()
    query.awaitTermination()

}}