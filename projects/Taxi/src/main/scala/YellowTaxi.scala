import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{DataFrame, SparkSession}

object YellowTaxi extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("YTaxi")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  spark.sparkContext.setLogLevel("ERROR")

  val path = "src/main/resources/"

  val taxiDF = spark.read.parquet(path + "parquet/taxi.parquet").cache()
  taxiDF.show(false)
  val locDF = spark.read.parquet(path + "parquet/location.parquet").cache()
  locDF.show(false)

//  spark.time(popularTrips(taxiDF).show())

  def popularTrips(df: DataFrame): DataFrame = {
    val nDF = df.withColumn("mile_price", (col("total_amount") - col("tip_amount")) / col("trip_distance"))
    val percentile = nDF.stat.approxQuantile("mile_price", Array(0.1, 0.9), 0.0001)
    percentile.foreach(println)
    nDF.where(col("mile_price") > percentile(0) && col("mile_price") < percentile(1))
      .groupBy(col("DOLocationID"))
      .agg(
        count("DOLocationID").alias("num_trips"),
        avg("mile_price").cast(DecimalType(5, 2)).alias("avg_mile_price")
      ).sort(desc("num_trips"))
      .limit(10)
  }

  def filterTaxi(df: DataFrame): DataFrame = {
    df.filter(
      col("tpep_pickup_datetime")
        .between(to_date(lit("2021-01-01")), to_date(lit("2021-07-31"))) &&
        col("total_amount") < 10000 &&
        col("fare_amount") > 0
    )
  }

  def pickup_date(df: DataFrame): DataFrame = {
    df.withColumn("pickup_date", date_trunc("Month", col("tpep_pickup_datetime")))
      .where(col("pickup_date")
        .between(to_date(lit("2021-01-01")), date_sub(to_date(lit("2021-08-01")), 1)))
      .cache()
  }
}
