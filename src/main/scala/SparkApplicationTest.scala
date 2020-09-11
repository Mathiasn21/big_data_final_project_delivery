import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_replace}

object SparkApplicationTest extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Testing App")
    .getOrCreate()
  print("\n\n\n")
  val file = Files.Movies
  val path = DataFiles.getFilePath(file)
  val format = DataFiles.getFileType(file)

  var df = spark.read.format(format).option("delimiter", "\t").option("header", value = true).option("inferSchema", value = true).load(path)
  df = df.withColumn("endYear", regexp_replace(col("endYear"), "\\\\N", "NULL"))
  df.write.option("overwrite", value = true).format("csv").save("D:\\data\\testing")
  df.show()
  spark.stop()
}
