package corporate

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.jackson.JsonMethods.parse

import scala.io.Source

object DMSql extends DataMart with App {
  val spark = SparkSession.builder()
    .master("local[4]")
    .appName("DMSql")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val FilePath(schema, ip, url, json) = args(0)
  override val conf = parse(readText(schema + ip, url+json))
  override val path = url

  spark.read.parquet(pqt + "account")
    .createOrReplaceTempView("account")
  spark.read.parquet(pqt + "client")
    .createOrReplaceTempView("client")
  spark.read.parquet(pqt + "operation")
    .createOrReplaceTempView("operation")
  spark.read.parquet(pqt + "rate")
    .createOrReplaceTempView("rate")
  readList(csv, "list1").union(readList(csv, "list2"))
    .createOrReplaceTempView("techTable")

  val corpPayments: DataFrame = readSQL(path + "/sql/corporate_payments.sql").cache()
  corpPayments.createOrReplaceTempView("corporate_payments")
  val corpAccount: DataFrame = readSQL(path + "/sql/corporate_account.sql").cache()
  corpAccount.createOrReplaceTempView("corporate_account")
  val corpInfo: DataFrame = readSQL(path + "/sql/corporate_info.sql").cache()
  corpInfo.createOrReplaceTempView(path + "corporate_info")

  List(corpPayments, corpAccount, corpInfo).foreach(df => spark.time(df.show(false)))

  def readSQL(way: String): DataFrame = {
    val src = Source.fromFile(way)
    val corporate = spark.sql(src.mkString)
    src.close()
    corporate
  }

  def readList(csv: String, name: String): DataFrame = {
    import org.apache.spark.sql.functions.typedLit
    import spark.implicits._

    val src: Source = Source.fromFile(csv + name + ".txt", enc = "cp1251")
    val str: Seq[String] = src.mkString
      .replace("\\", "\\\\").split(",")
    src.close()
    str.toDF("val")
      .withColumn("name", typedLit(name))
  }
}

/*
  def readList(name: String): DataFrame = {
    import org.apache.spark.sql.functions.typedLit
    import spark.implicits._
    spark.sparkContext.textFile(csv + name + ".txt")
      .collect().mkString
      .replace("\\", "\\\\").split(",")
      .toSeq
      .toDF("val")
      .withColumn("name", typedLit(name))
  }
*/

