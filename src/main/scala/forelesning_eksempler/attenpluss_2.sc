import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, column}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.functions._

Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)

val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()

//Special import that comes after init of spark session
//import spark.implicits._

val df = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("C:\\Users\\marpe\\Documents\\bigdata_data\\tv-shows.csv")


val condition = (column: Column, value: String) =>
    count(when(column === 1 && col("Age") === value, 1)).as(column.toString() + " " + value)

val countDf =df.agg(
  condition(col("Prime Video"), "18+"),
  condition(col("Netflix"), "18+"),
  condition(col("Hulu"), "18+"),
  condition(col("Disney+"), "18+"),
  condition(col("Prime Video"), "all"),
  condition(col("Netflix"), "all"),
  condition(col("Hulu"), "all"),
  condition(col("Disney+"), "all"))


val cols = countDf.columns
val split = cols.splitAt(cols.length / 2)
val predicate = (c:String) => struct(col(c).as("v"), lit(c).as("k"))
val structCols18 = split._1.map(predicate)
val structColsAll = split._2.map(predicate)

val max_df = countDf
  .withColumn("maxCol 18+", greatest(structCols18: _*)
  .getItem("k"))
  .withColumn("maxCol All", greatest(structColsAll: _*)
    .getItem("k"))

max_df.show()