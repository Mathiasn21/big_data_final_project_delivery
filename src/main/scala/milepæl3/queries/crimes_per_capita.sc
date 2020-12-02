import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()


//Which city had the highest crime rate per capita?

val sc = spark.sparkContext
val filePath = "D:\\data\\crime_in_context_19752015.csv"

var file = sc.textFile(filePath)
val headers = file.first()
val head = headers.split(",")

file = file.filter(line => line != headers && !line.contains("United States"))
val splitFile = file.map(line => {
  line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", head.length)
})
splitFile.collect().foreach ( x => println(x.mkString(", ")))

//Function that converts String to Double
val convertToDouble = (str: String) => {
  var i = 0.0
  if (!(str == null) && !str.isBlank) {
    i = str.toDouble
  }
  i
}

//Creates a key-value RDD by mapping
val keyValueRDD = splitFile.map(arr => (arr(2), convertToDouble(arr(10))))
//combines values with the same key
val reducedRDD = keyValueRDD.reduceByKey ((a, b) => a + b)

//Function that sorts reducedRDD and then picks the max value
val maxKey2 = reducedRDD.max()(new Ordering[(String, Double)]() {
  override def compare(x: (String, Double), y: (String, Double)): Int =
    Ordering[Double].compare(x._2, y._2)
})

print(reducedRDD.toDebugString)
sc.parallelize(Seq(maxKey2)).saveAsTextFile("/data/max")
print(maxKey2)
