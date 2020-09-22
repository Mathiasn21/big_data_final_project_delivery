import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession}
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

//Special import that comes after init of spark session
import spark.implicits._

def loadDf(file: String): DataFrame = (spark.read format "csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", ",")
  .load(file)

val guns_file = "D:\\data\\guns.csv"
val drug_file = "D:\\data\\drug_deaths.csv"

val gunDf = loadDf(guns_file)
val drugDf = loadDf(drug_file).filter(!col("_c0").contains("("))

drugDf.show(false)
gunDf.show(false)

val white = "White"
val black = "Black"
val race_col = col("Race")
val predicate = race_col.contains(white) or race_col.contains(black)

val countByRace = (column: Column, value: String, alias: String) =>
  count(when(column.contains(value), 1)).as(alias)

val overdoseByRace = drugDf.filter(predicate)
  .agg(
    countByRace(race_col, black, "Overdose_Black_count"),
    countByRace(race_col, white, "Overdose_White_count")
  )

val gunDeathByRace = gunDf.filter(predicate)
  .agg(
    countByRace(race_col, black, "Gun_death_Black_count"),
    countByRace(race_col, white, "Gun_death_White_count")
  )

overdoseByRace.join(gunDeathByRace).explain(true)