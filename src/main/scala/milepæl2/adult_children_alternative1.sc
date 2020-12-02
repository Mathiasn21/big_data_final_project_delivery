import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{Column, SaveMode, SparkSession}

Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)

val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()

/*
    Which streaming service releases most adult (18+) series
    and which service releases most series for children?
    Alternative 1
 */
val df = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("C:\\Users\\marpe\\Documents\\bigdata_data\\tv-shows.csv")

/*Anonymous function that takes a column and String Value as parameters.
  This function counts if the column is of value 1, meaning it has the streaming service available,
  and the "Age" column is a certain value
  The count column gets an alias of the column names and "Age" values
 */
val condition = (column: Column, value: String) =>
    count(when(column === 1 && col("Age") === value, 1)).as(column.toString() + " " + value)

//Passes the streaming services and age values into the anonymous function
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
//splits the columns in two
val split = cols.splitAt(cols.length / 2)

//Separates the streaming services for 18+ and "all"
val predicate = (c:String) => struct(col(c).as("v"), lit(c).as("k"))
val structCols18 = split._1.map(predicate)
val structColsAll = split._2.map(predicate)

//Puts the streaming services with the most adult and children series in a new column
val maxDf = countDf
  .withColumn("maxCol 18+", greatest(structCols18: _*)
  .getItem("k"))
  .withColumn("maxCol All", greatest(structColsAll: _*)
    .getItem("k"))

//max_df.show()
maxDf.write.mode(SaveMode.Overwrite).format("csv").save("D:\\data\\testing")

