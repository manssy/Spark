import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object CsvToParquet extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("taxiParquet")
    .getOrCreate()

  //spark.sparkContext.setLogLevel("ERROR")

  val path = "src/main/resources/"

  val taxiDF = readerCSV("trip", 12).cache()
  taxiDF.write.partitionBy("VendorID")
    .mode(SaveMode.Overwrite).parquet(path + "/parquet/taxi.parquet")

  val locationDF = readerCSV("taxi+_zone_lookup.csv", 4).cache()
  locationDF.write
    .mode(SaveMode.Overwrite).parquet(path + "/parquet/location.parquet")

  def readerCSV(name: String, num: Int): DataFrame = {
    val df = spark.read
      .options(Map("inferSchema" -> "true", "header" -> "true"))
      .csv(path + name)
      .repartition(num)
    df.printSchema()
    df.show(false)
    df
  }
}
