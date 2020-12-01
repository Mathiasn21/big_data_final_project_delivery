import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{Column, SaveMode, SparkSession}



Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)


val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()


import spark.implicits._

val sc = spark.sparkContext
val filePath = "D:\\data\\crime_in_context_19752015.csv"

val df = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load(filePath)

var file = sc.textFile(filePath)
val headers = file.first()
val head = headers.split(",")
file = file.filter(line => line != headers && !line.contains("United States"))
val splitFile = file.map(line => {
  line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", head.length)
})
splitFile.collect().foreach ( x => println(x.mkString(", ")))

print(splitFile.toDebugString)

splitFile.collect().foreach(println)
