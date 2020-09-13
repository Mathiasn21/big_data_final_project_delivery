import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, column}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.functions._

Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)

val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()

val df = spark.read.format("csv")
  .option("header", value = true)
  .option("inferSchema", "true")
  .load("C:\\Users\\marpe\\Documents\\tv-shows.csv")

val myDf = df.select("Title", "IMDb", "Netflix", "Hulu", "Prime Video", "Disney+").sort(desc("IMDb")).limit(10)

val str = myDf.columns.tail.map(
  c => struct(col(c).as("c"), lit(c).as("k"))
)

myDf.withColumn("Streaming Service", when(col("Prime Video") === 1, )

)


/*myDf.select(col("Title"), col("IMDb"),
  when(col("Prime Video")  ===1, "Prime Video")
    .when(col("Netflix") === 1, "Netflix")
    .when(col("Hulu") === 1, "Hulu")
    .when(col("Disney+") === 1, "Disney+")
    .alias("Streaming service")).show()*/
