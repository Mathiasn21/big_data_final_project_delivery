val filePath = "crime_in_context_19752015.csv"
var file = sc.textFile(filePath)
file.collect().foreach(println)
val headers = file.first()
val head = headers.split(",")
file = file.filter(line => line != headers && !line.contains("United States"))
val splitFile = file.map(line => {
  line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", head.length)
})
splitFile.collect().foreach ( x => println(x.mkString(", ")))

import scala.collection.mutable.ListBuffer

val convertToDouble = (str: String) => {
  var i = 0.0
  if (!(str == null) && !str.isBlank) {
    i = str.toDouble
  }
  i
}

val mapped = splitFile.map(arr => (arr(2), convertToDouble(arr(10))))

val reducedRDD = mapped.reduceByKey ((a, b) => (a + b))

val maxValue = reducedRDD.max

