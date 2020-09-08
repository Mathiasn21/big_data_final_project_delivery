import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, column}
import org.apache.spark.sql.{SaveMode, SparkSession}



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

//VIKTIGSTE :')
var df = spark.read.format(format)
  .option("delimiter", ",")
  .option("header", value = true)
  .option("inferSchema", value = true)
  .load(path)
  .drop("type")

df.write.mode(SaveMode.Overwrite).format("csv").save("D:\\data\\testing")
print(df.columns.mkString(", "))
df.where("Year > 2010").sort(column("IMDb").desc_nulls_last).show(1000, truncate = false)
df.select("Age").distinct().show()

df.withColumn("l_c0", col("_c0").cast("long")).show()
print(df.columns.mkString(", "))

df.printSchema()

/*
df.select(
  when( (col("armed") === "gun") && ($"race" === $"Black"), "Yes" )
    .otherwise("No")
).show()
*/

