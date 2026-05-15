//> using scala 2.13.18
//> using dep org.apache.spark::spark-sql:4.1.1
//> using javaOpt --add-opens=java.base/java.lang=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.io=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.net=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.nio=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.util=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/sun.nio.cs=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/sun.security.action=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
//> using javaOpt --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.try_to_timestamp
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

object Main {
  private val DefaultInputPath = "data/input/transactions.csv"
  private val DefaultOutputPath = "target/spark-output/transaction-summary"
  private val DefaultMaster = "local-cluster[3,1,4096]"
  private val AnalysisPartitions = "12"
  private val ExecutorMemory = "4g"
  private val SparkJavaOptions = Seq(
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
  )

  private val TransactionSchema = StructType(
    Seq(
      StructField("transaction_id", StringType, nullable = false),
      StructField("customer_id", StringType, nullable = false),
      StructField("event_ts", StringType, nullable = false),
      StructField("region", StringType, nullable = false),
      StructField("country", StringType, nullable = false),
      StructField("product_id", StringType, nullable = false),
      StructField("product_category", StringType, nullable = false),
      StructField("quantity", IntegerType, nullable = false),
      StructField("unit_price", DoubleType, nullable = false),
      StructField("discount_pct", DoubleType, nullable = false),
      StructField("payment_method", StringType, nullable = false),
      StructField("status", StringType, nullable = false),
      StructField("_corrupt_record", StringType, nullable = true)
    )
  )

  final case class JobConfig(
      inputPath: String,
      outputPath: String,
      master: String
  )

  def main(args: Array[String]): Unit =
    parseArgs(args) match {
      case Left(message) =>
        System.err.println(message)
        System.exit(1)

      case Right(config) =>
        val sparkBuilder = SparkSession
          .builder()
          .appName("Large CSV Transaction Pipeline")
          .master(config.master)
          .config("spark.sql.shuffle.partitions", AnalysisPartitions)
          .config("spark.default.parallelism", AnalysisPartitions)
          .config("spark.executor.instances", "3")
          .config("spark.executor.memory", ExecutorMemory)
          .config(
            "spark.driver.extraJavaOptions",
            SparkJavaOptions.mkString(" ")
          )
          .config(
            "spark.executor.extraJavaOptions",
            SparkJavaOptions.mkString(" ")
          )
          .config("spark.executorEnv.SPARK_SCALA_VERSION", "2.13")
          .config("spark.sql.parquet.compression.codec", "gzip")

        val configuredBuilder =
          if (config.master.startsWith("local")) {
            sparkBuilder
              .config("spark.driver.host", "127.0.0.1")
              .config("spark.driver.bindAddress", "127.0.0.1")
          } else {
            sparkBuilder
          }

        val spark = configuredBuilder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        try {
          runPipeline(spark, config)
        } finally {
          spark.stop()
        }
    }

  private def parseArgs(args: Array[String]): Either[String, JobConfig] =
    args.toList match {
      case Nil =>
        Right(JobConfig(DefaultInputPath, DefaultOutputPath, DefaultMaster))
      case "--help" :: Nil =>
        Left(usage)
      case inputPath :: outputPath :: Nil =>
        Right(JobConfig(inputPath, outputPath, DefaultMaster))
      case inputPath :: outputPath :: master :: Nil =>
        Right(JobConfig(inputPath, outputPath, master))
      case _ =>
        Left(usage)
    }

  private def usage: String =
    s"""Usage: sbt "run [input_csv] [output_dir] [spark_master]"
       |
       |Defaults:
       |  input_csv    $DefaultInputPath
       |  output_dir   $DefaultOutputPath
       |  spark_master $DefaultMaster
       |
       |Expected CSV header:
       |  transaction_id,customer_id,event_ts,region,country,product_id,product_category,quantity,unit_price,discount_pct,payment_method,status
       |""".stripMargin

  private def runPipeline(spark: SparkSession, config: JobConfig): Unit = {
    val rawTransactions = readTransactions(spark, config.inputPath)
    val cleanedTransactions = transformTransactions(rawTransactions)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val rawCount = rawTransactions.count()
    val cleanedCount = cleanedTransactions.count()
    val summary = summarizeTransactions(cleanedTransactions)

    summary.write
      .mode("overwrite")
      .option("compression", "gzip")
      .partitionBy("event_date")
      .parquet(config.outputPath)

    println(s"Read $rawCount CSV rows from ${config.inputPath}")
    println(s"Wrote $cleanedCount valid rows into ${config.outputPath}")

    cleanedTransactions.unpersist()
  }

  private def readTransactions(
      spark: SparkSession,
      inputPath: String
  ): DataFrame =
    spark.read
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .schema(TransactionSchema)
      .csv(inputPath)

  private def transformTransactions(transactions: DataFrame): DataFrame =
    transactions
      .filter(col("_corrupt_record").isNull)
      .filter(col("transaction_id").isNotNull)
      .filter(col("customer_id").isNotNull)
      .filter(col("quantity") > 0)
      .filter(col("unit_price") >= 0.0)
      .filter(col("discount_pct").between(0.0, 1.0))
      .withColumn(
        "event_time",
        try_to_timestamp(col("event_ts"), lit("yyyy-MM-dd'T'HH:mm:ss"))
      )
      .filter(col("event_time").isNotNull)
      .withColumn("event_date", to_date(col("event_time")))
      .withColumn("gross_amount", col("quantity") * col("unit_price"))
      .withColumn(
        "net_amount",
        round(col("gross_amount") * (lit(1.0) - col("discount_pct")), 2)
      )
      .drop("_corrupt_record")

  private def summarizeTransactions(transactions: DataFrame): DataFrame =
    transactions
      .groupBy(
        col("event_date"),
        col("region"),
        col("country"),
        col("product_category"),
        col("status")
      )
      .agg(
        count(lit(1)).as("transaction_count"),
        countDistinct(col("customer_id")).as("unique_customers"),
        sum(col("quantity")).as("units_sold"),
        round(sum(col("gross_amount")), 2).as("gross_revenue"),
        round(sum(col("net_amount")), 2).as("net_revenue")
      )
      .orderBy(
        col("event_date"),
        col("region"),
        col("country"),
        col("product_category"),
        col("status")
      )
}
