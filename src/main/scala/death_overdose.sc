import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, column}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.functions._

Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)

val spark = SparkSession.builder
  .master("local[*]")
  .appName("Testing App")
  .getOrCreate()

def loadDf(file: String): DataFrame = (spark.read format "csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load(file)

val guns_file = "D:\\data\\guns.csv"
val drug_file = "D:\\data\\drug_deaths.csv"

val gunDf = loadDf(guns_file)
val drugDf = loadDf(drug_file)

val white = "White"
val black = "Black"
val race_col = col("Race")
val predicate = race_col.contains(white) or race_col.contains(black)

val count_race = count(col("Race"))

val overdoseByRace = drugDf.filter(predicate)
  .agg(
    count(when(race_col.contains(black), 1)).as("Overdose_Black_count"),
    count(when(race_col.contains(white), 1)).as("Overdose_White_count")
  )

overdoseByRace.show()