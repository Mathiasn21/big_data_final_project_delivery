import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession



Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)


val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()

val sc = spark.sparkContext
val filePath = "D:\\data\\crime_in_context_19752015.csv"

var file = sc.textFile(filePath)
val headers = file.first()
val head = headers.split(",")

//Handle headers
file = file.filter(line => line != headers && !line.contains("United States"))

//Manually split each line into columns, returns RDD[Array[String]]
val splitFile = file.map(line => {
  line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", head.length)
})
splitFile.collect().foreach ( x => println(x.mkString(", ")))

print(splitFile.toDebugString)
