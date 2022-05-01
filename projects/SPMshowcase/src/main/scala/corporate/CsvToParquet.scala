package corporate

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.json4s.jackson.JsonMethods.parse

object CsvToParquet extends DataMart with App {
  val spark = SparkSession.builder()
//    .master("local[4]")
    .appName("CSV").getOrCreate()

//  spark.conf.set("spark.sql.shuffle.partitions",10)
  spark.sparkContext.setLogLevel("ERROR")

  val FilePath(schema, ip, url, json) = args(0)
  override val conf = parse(readText(schema + ip, url + json))
  override val path = url

  val AccountSchema = StructType(Array(
    StructField("AccountID", IntegerType),
    StructField("AccountNum", DecimalType(20, 0)),
    StructField("ClientId", IntegerType),
    StructField("DateOpen", DateType)
  ))
  val ClientSchema = StructType(Array(
    StructField("ClientId", IntegerType),
    StructField("ClientName", StringType),
    StructField("Type", StringType),
    StructField("Form", StringType),
    StructField("RegisterDate", DateType)
  ))
  val OperationSchema = StructType(Array(
    StructField("AccountDB", IntegerType),
    StructField("AccountCR", IntegerType),
    StructField("DateOp", DateType),
    StructField("Amount", DoubleType),
    StructField("Currency", StringType),
    StructField("Comment", StringType)
  ))
  val RateSchema = StructType(Array(
    StructField("Currency", StringType),
    StructField("Rate", DoubleType),
    StructField("RateDate", DateType)
  ))

  csvToPqt("account", "DateOpen", AccountSchema)
  csvToPqt("client", "RegisterDate", ClientSchema)
  csvToPqt("operation", "DateOp", OperationSchema)
  csvToPqt("rate", "RateDate", RateSchema)

  def csvToPqt(name: String, partition: String, schema: StructType): Unit = {
    val df = spark.read.options(Map("delimiter" -> ";", "header" -> "true", "encoding" -> "cp1251")).schema(schema)
      .csv(csv + name + ".csv").repartition(4)
    df.repartition(1).write.partitionBy(partition)
      .mode(SaveMode.Overwrite).parquet(pqt + name)
  }
}
