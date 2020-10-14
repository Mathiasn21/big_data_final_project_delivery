package milepæl3.hdfs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object hdfs{
    def main(args:Array[String]):Unit= {
        Logger.getLogger("org").setLevel(Level.WARN)
        Logger.getLogger("akka").setLevel(Level.WARN)

        val path = "hdfs://127.0.0.1:19000/crime_in_context_19752015.csv"
        val spark = SparkSession.builder()
          .master("local[*]")
          .appName("HDFS")
          .getOrCreate()

        val df = spark.read.format("csv")
          .option("delimiter", ",")
          .option("header", value = true)
          .option("inferSchema", value = true)
          .load(path)
          .drop("type")

        df.describe().show()
        spark.stop()
    }
}
