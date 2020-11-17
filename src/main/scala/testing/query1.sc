import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.{col, regexp_extract, regexp_replace, when}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)

val filePath = "C:\\Users\\Nilse\\Desktop\\grades_copy.csv"

val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()

var dataframe = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", ";")
  .load(filePath)

val pattern = "([0-9]+pts)"

dataframe
  .withColumn("Comments lab 7", regexp_extract(dataframe.col("Comments lab 7"), pattern, 0))
  .na.fill(0, Seq("Comments lab 7"))
  .withColumn("Comments lab 8", regexp_extract(dataframe.col("Comments lab 8"), pattern, 0))
  .na.fill(0, Seq("Comments lab 8"))
  .withColumn("Comments lab 9", regexp_extract(dataframe.col("Comments lab 9"), pattern, 0))
  .na.fill(0, Seq("Comments lab 9"))
  .withColumn("Comments lab 10", regexp_extract(dataframe.col("Comments lab 10"), pattern, 0))
  .na.fill(0, Seq("Comments lab 10"))
  .withColumn("Comments lab 11", regexp_extract(dataframe.col("Comments lab 11"), pattern, 0))
  .na.fill(0, Seq("Comments lab 11"))
  .na.fill(0, Seq("pts"))
  .repartition(1)
  .write
  .mode(SaveMode.Overwrite)
  .format("csv")
  .option("delimiter", ",")
  .option("header","true")
  .save("C:\\Users\\Nilse\\Desktop\\grades")
