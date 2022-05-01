package postgres

import corporate.DataMart
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.json4s.jackson.JsonMethods.parse

import java.util.Properties

object Postgresql extends DataMart with App {
  val spark = SparkSession.builder()
//    .master("local[4]")
    .appName("Postgres")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val FilePath(schema, ip, url, json) = args(0)
  override val conf = parse(readText(schema + ip, url+json))
  override val path = url

  private val connect = new Properties()
  connect.put("user", (conf \ "pg.user").values.toString)
  connect.put("password", (conf \ "pg.password").values.toString)
  connect.put("driver", "org.postgresql.Driver")

  val pgSchema = (conf \ "pg.schema").values.toString
  val pgUrl = (conf \ "pg.url").values.toString

  pgWrite("corporate_payments")
  pgWrite("corporate_account")
  pgWrite("corporate_info")

  private def pgWrite(name: String): Unit = {
    spark.read.parquet(pqt + name)
      .write.mode(SaveMode.Overwrite)
      .jdbc(pgUrl, pgSchema + "." + name, connect)
  }
}
