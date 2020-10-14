package milepæl3.hdfs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object hdfs{
    def main(args:Array[String]):Unit= {
        Logger.getLogger("org").setLevel(Level.WARN)
        Logger.getLogger("akka").setLevel(Level.WARN)

        val path = "hdfs://10.0.0.95:9000/crime_in_context_19752015.csv"
        val pathOut = "hdfs://10.0.0.95:9000/crime_stats/crime_in_protland.csv"
        val spark = SparkSession.builder()
          .master("local[*]")
          .appName("HDFS")
          .getOrCreate()

        import spark.implicits._

        val sc = spark.sparkContext

        var file = sc.textFile(path)

        val headers = file.first()
        val head = headers.split(",")
        file = file.filter(line => line != headers && !line.contains("United States"))

        val indexOfCity = head.indexWhere(str => str.equals("agency_jurisdiction"))
        val crimeFirst = head.indexWhere(str => str.equals("homicides_percapita"))
        val crimeLast = head.indexWhere(str => str.equals("robberies_percapita"))
        val yearIndex = head.indexWhere(str => str.equals("report_year"))

        val splitFile = file.map(line => {
            line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", head.length)
        })

        var map = mutable.HashMap[Int, String](-1 -> "")
        for (i <- head.indices) {
            map += (i -> head(i))
        }

        val filtered = splitFile.filter(arr => arr(indexOfCity).contains("Portland"))

        val convertToDouble = (arr: Array[String]) => {
            val convertedArr = ListBuffer[Double]()
            for (i <- crimeFirst to crimeLast) {
                val str = arr(i)
                if(str == null || str.isBlank){
                    convertedArr += 0
                }else{
                    convertedArr += str.toDouble
                }
            }
            convertedArr.toArray
        }

        val convertToInt = (str: String) => {
            var i = 0
            if (!(str == null) && !str.isBlank) {
                i = str.toInt
            }
            i
        }

        var maxValIndex = (arr: Array[Double]) => {
            var max = 0.0
            var maxIndex = -1
            for (i <- arr.indices) {
                val d = arr(i)
                if(max < d){
                    max = d
                    maxIndex = i
                }
            }
            maxIndex = if(maxIndex == -1) maxIndex else maxIndex + crimeFirst
            (max, map(maxIndex))
        }

        //Find most common crime in portland.
        val summedRDD = filtered.map(
            arr => (
              convertToInt(arr(yearIndex)),
              arr(indexOfCity).replaceAll("[\",]", ""),
              maxValIndex(convertToDouble(arr))
            )
        )

        summedRDD.collect().foreach(arr => {
            println(arr._1, arr._2, arr._3._1, arr._3._2)
        })

        //Write res to disk:
        val finalRDD = summedRDD.map(row => {
            val str = row._1 + "," + row._2 +
              "," + row._3._2 + "," + row._3._1
            str
        })
        finalRDD.coalesce(1).saveAsTextFile(pathOut)
        spark.stop()
    }
}
