import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()

import spark.implicits._

val sc = spark.sparkContext
val filePath = "D:\\data\\crime_in_context_19752015.csv"
//val filePath = "crime_in_context_19752015.csv"

val file = sc.textFile(filePath)

val headers = file.map(line => {
  line.split(",")
})

val head = headers.first()

print(head.mkString(", "))


val indexOfCity = head.indexWhere(str => str.equals("agency_jurisdiction"))
val crime_first = head.indexWhere(str => str.equals("homicides_percapita"))
val crime_last = head.indexWhere(str => str.equals("robberies_percapita"))

val splitFile = file.map(line => {
  line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", head.length)
})

val filtered = splitFile.filter(arr => arr(indexOfCity).contains("Portland"))

val convert = (arr: Array[String]) => {
  val convertedArr = ListBuffer[Double]()
  for (i <- crime_first to crime_last) {
    val str = arr(i)
    if(str == null || str.isBlank){
      convertedArr += 0
    }else{
      convertedArr += str.toDouble
    }
  }
  convertedArr.toArray
}


val t = filtered.map(arr => (arr(indexOfCity), convert(arr)))
t.collect().foreach(arr => {
  println(arr._1, arr._2.mkString("Array(", ", ", ")"))
})
//Find most common crime in portland.