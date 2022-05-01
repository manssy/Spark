package corporate

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.json4s.jackson.JsonMethods.parse

object DMSpark extends DataMart with App {
  val spark = SparkSession.builder()
//    .master("local[4]")
    .appName("DMSpark")
    .getOrCreate()

  spark.conf.set("spark.sql.shuffle.partitions", 10)
  spark.sparkContext.setLogLevel("ERROR")

  val FilePath(schema, ip, url, json) = args(0)
  override val conf = parse(readText(schema + ip, url + json))
  override val path = url

  val account: DataFrame = spark.read.parquet(pqt + "account").cache()
  val client: DataFrame = spark.read.parquet(pqt + "client").cache()
  val operation: DataFrame = spark.read.parquet(pqt + "operation").cache()
  val rate: DataFrame = spark.read.parquet(pqt + "rate").cache()

  /*  val op: DataFrame =
      operation.alias("o").join(rate.alias("r"), operation("Currency") === rate("Currency") && operation("DateOp") >= rate("RateDate"), "inner")
        .select(
          col("AccountDB"), col("AccountCR"), (col("Amount") * col("Rate")).alias("amt"),
          col("Comment"), col("DateOp"),
          rank.over(Window.partitionBy(
            col("AccountDB"), col("DateOp"), col("o.currency")
          ).orderBy(col("RateDate").desc)).alias("rss")
        ).filter("rss = 1")
        .cache()*/

  val op = operation.join(rate, rate("Currency") === operation("Currency") &&
    rate("RateDate") === operation("DateOp"), "fullouter")
    .select(operation("AccountDB"), operation("AccountCR"), operation("Amount"),
      coalesce(rate("Currency"), operation("Currency")).as("Currency"), operation("Comment"),
      coalesce(operation("DateOp"), rate("RateDate")).as("DateOp"), rate("Rate"))
    .withColumn("amt", operation("Amount") *
      last(col("Rate"), ignoreNulls = true).over(Window.orderBy("Currency", "DateOp")))
    .cache()

  val acc: DataFrame =
    operation.select(col("AccountDB").alias("Account"), col("DateOp").alias("CutoffDt")).distinct()
      .union(
        operation.select(col("AccountCR").alias("Account"), col("DateOp").alias("CutoffDt")).distinct())
      .join(account, col("Account") === col("AccountID"), "inner")
      .select(col("AccountID"), col("AccountNum"), col("ClientId"), col("CutoffDt"))

  val paymentAmt: DataFrame =
    op.groupBy(col("AccountDB"), col("DateOp"))
      .agg(round(sum(col("amt"))).alias("PaymentAmt"))
  val enrollmentAmt: DataFrame =
    op.groupBy(col("AccountCR"), col("DateOp"))
      .agg(round(sum(col("amt"))).alias("EnrollmentAmt"))

  val amt: DataFrame =
    acc.join(paymentAmt, acc("AccountID") === paymentAmt("AccountDB") and acc("CutoffDt") === paymentAmt("DateOp"), "left")
      .select(acc("AccountID"), acc("AccountNum"), acc("ClientID"), acc("CutoffDt"), paymentAmt("AccountDB"), paymentAmt("PaymentAmt"))
      .join(enrollmentAmt, acc("AccountID") === enrollmentAmt("AccountCR") and acc("CutoffDt") === enrollmentAmt("DateOp"), "left")
      .select(
        col("AccountID"), col("AccountNum"), col("ClientID"), col("CutoffDt"),
        col("AccountDB"), coalesce(col("PaymentAmt"), lit(0)).alias("PaymentAmt"),
        col("AccountCR"), coalesce(col("EnrollmentAmt"), lit(0)).alias("EnrollmentAmt"))
      .cache()

  val taxAmt: DataFrame =
    op.filter(op("AccountCR").isin(account.filter(account("AccountNum").like("40702%"))
      .select("AccountID").distinct().rdd.map(x => x(0)).collect(): _*))
      .groupBy(op("AccountDB"))
      .agg(round(sum(col("amt"))).alias("TaxAmt"))

  val clearAmt: DataFrame =
    op.filter(op("AccountDB").isin(account.filter(account("AccountNum").like("40802%"))
      .select("AccountID").distinct().rdd.map(x => x(0)).collect(): _*))
      .groupBy(op("AccountCR"))
      .agg(round(sum(col("amt"))).alias("ClearAmt"))

  val carsAmt =
    op.where(!op("comment").rlike(readList(csv + "list1.txt")))
      .groupBy(op("AccountDB"), op("DateOp"))
      .agg(round(sum(op("amt"))).alias("CarsAmt"))
  val foodAmt: DataFrame =
    op.where(op("comment").rlike(readList(csv + "list2.txt")))
      .groupBy(op("AccountCR"), op("DateOp"))
      .agg(round(sum(op("amt"))).alias("FoodAmt"))

  val flAmt: DataFrame = account.join(client, account("ClientID") === client("ClientID") && client("type") === "Ô", "inner")
    .join(op, op("AccountCR") === account("AccountID"), "inner")
    .groupBy(col("AccountDB"))
    .agg(round(sum(col("amt"))).alias("FLAmt"))

  val corpPayments: DataFrame = amt.alias("a")
    .join(taxAmt.alias("t"), col("a.AccountDB") === col("t.AccountDB"), "left").select("*")
    .join(clearAmt.alias("c"), col("a.AccountCR") === col("c.AccountCR"), "left").select("*")
    .join(carsAmt.alias("ca"), col("a.AccountDB") === col("ca.AccountDB"), "left").select("*")
    .join(foodAmt.alias("fo"), col("a.AccountCR") === col("fo.AccountCR"), "left").select("*")
    .join(flAmt.alias("f"), col("a.AccountDB") === col("f.AccountDB"), "left")
    .select(
      col("AccountID"), col("ClientID"), col("PaymentAmt"), col("EnrollmentAmt"),
      coalesce(col("TaxAmt"), lit(0)).alias("TaxAmt"),
      coalesce(col("ClearAmt"), lit(0)).alias("ClearAmt"),
      coalesce(col("CarsAmt"), lit(0)).alias("CarsAmt"),
      coalesce(col("FoodAmt"), lit(0)).alias("FoodAmt"),
      coalesce(col("FLAmt"), lit(0)).alias("FLAmt"),
      col("a.CutoffDt"))
    .cache()

  val corpAccount: DataFrame =
    corpPayments.join(account, corpPayments("AccountId") === account("AccountId"), "inner").select("*")
      .join(client, corpPayments("ClientID") === client("ClientID"), "inner")
      .select(
        corpPayments("AccountId"), col("AccountNum"), col("DateOpen"), corpPayments("ClientId"),
        client("ClientName"), (corpPayments("PaymentAmt") + corpPayments("EnrollmentAmt")).alias("TotalAmt"),
        corpPayments("CutoffDt"))
      .cache()

  val corpInfo: DataFrame =
    client.join(corpAccount, client("ClientId") === corpAccount("ClientId"))
      .groupBy(client("ClientId"), client("ClientName"), client("Type"),
        client("Form"), client("RegisterDate"), corpAccount("CutoffDt"))
      .agg(sum(corpAccount("TotalAmt")).alias("TotalAmt"))
      .cache()

//  List(corpPayments, corpAccount, corpInfo).foreach(df => spark.time(df.orderBy("ClientID").show(false)))

  corpPayments.repartition(1).write.partitionBy("CutoffDt")
    .mode(SaveMode.Overwrite).parquet(pqt + "corporate_payments")
  corpAccount.repartition(1).write.partitionBy("CutoffDt")
    .mode(SaveMode.Overwrite).parquet(pqt + "corporate_account")
  corpInfo.repartition(1).write.partitionBy("CutoffDt")
    .mode(SaveMode.Overwrite).parquet(pqt + "corporate_info")

  def readList(way: String): String = {
    import spark.implicits._
    spark.sparkContext.textFile(way)
      .collect().mkString
      .replace("\\", "\\\\")
      .replace("%", "").replace(" ", "")
      .split(",").toSeq
      .toDF().rdd.map(r => r(0)).collect().mkString("|")
  }
}
