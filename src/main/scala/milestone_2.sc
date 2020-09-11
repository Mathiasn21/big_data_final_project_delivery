import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)

val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()
print("\n\n\n")

val file = Files.TvShows
val path = DataFiles.getFilePath(file)
val format = DataFiles.getFileType(file)

var df = spark.read.format(format)
  .option("delimiter", ",")
  .option("header", value = true)
  .option("inferSchema", value = true)
  .load(path)
  .drop("type")

def query_1(df: sql.DataFrame): Unit ={
  //TODO: Find average num of shows pr year.
  val sortedCountByYear = df.groupBy("Year")
    .count()
    .sort("Year")

  val avgByYear = sortedCountByYear
    .agg(avg("count").as("Avg_year"))
  val medianType = sortedCountByYear
    .stat.approxQuantile("count", Array(0.5), 0.10)

  sortedCountByYear.show()
  avgByYear.show()
  print("\n\nMedian: " + medianType.mkString(", "))
}

def query_2(df : sql.DataFrame): Unit ={
  //TODO: Find 5s year has the highest rating shows pr year
  df.filter("IMDb is not null").groupBy("Year")
    .sum("IMDb").sort(column("sum(IMDb)").desc).show(5)

  val summedDf = df.agg(
    sum("Hulu").as("Hulu_sum"),
    sum("Disney+").as("Disney_sum"),
    sum("Netflix").as("Netflix_sum"),
    sum("Prime Video").as("Prime_Vid_sum")
  )
  summedDf.show()

  val structs = summedDf.columns.tail.map(
    c => struct(col(c).as("v"), lit(c).as("k"))
  )

  summedDf.withColumn("maxCol", greatest(structs: _*).getItem("k"))
    .withColumn("minCol", least(structs: _*).getItem("k")).show()

  print("\n")
}
