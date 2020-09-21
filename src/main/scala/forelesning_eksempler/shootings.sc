import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._


Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)

val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()
print("\n\n\n")


val file = Files.Shootings
val path = DataFiles.getFilePath(file)
val format = DataFiles.getFileType(file)

var df = spark.read.format(format)
  .option("delimiter", ",")
  .option("header", value = true)
  .option("inferSchema", value = true)
  .load(path)
  .drop("type")

def createWindow(df: DataFrame): Unit = {
  val window = Window
    .partitionBy("Race")
    .orderBy(col("date").asc)
    .rowsBetween(-365, Window.currentRow)
  val numShoot = count(col("Race")).over(window).as("count")
  df.select(col("date"), col("race"), numShoot).show()
}

createWindow(df)

