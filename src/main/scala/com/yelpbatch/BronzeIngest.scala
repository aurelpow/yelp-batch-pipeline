package com.yelpbatch

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * BronzeIngest - Spark application for ingesting raw Yelp dataset JSON files
 * into a Delta Lake bronze layer as part of a data lakehouse architecture.
 *
 * Input:
 *   - Yelp JSON files by table (e.g., yelp_academic_dataset_business.json)
 * Output:
 *   - Delta tables partitioned by `_ingest_date` under `bronzeDir/<table>`
 *
 * Usage:
 *   - Processes all tables listed in configuration, or a specific table via command line argument.
 */
object BronzeIngest {

  /**
   * Main entry point for the BronzeIngest application.
   *
   * @param args Command line arguments. If provided, the first argument can be a single table name to process.
   */
  def main(args: Array[String]): Unit = {

    // Initialize Spark session with application name for cluster monitoring
    val spark = SparkSession.builder
      .appName("BronzeIngest")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()


    // Set Spark log level to "ERROR" to suppress INFO and WARN output
    spark.sparkContext.setLogLevel("ERROR")

    // Load defaults from classpath resource (app.local.conf)
    val fileConf: Config =
      ConfigFactory.parseResources("app.local.conf").resolve()

    // --- Config (simple & safe defaults) ---
    val conf      = ConfigFactory.parseResources("app.local.conf").resolve()
    val srcDir    = conf.getString("paths.src")
    val bronzeDir = conf.getString("paths.bronzeDir")
    val tablesCsv = spark.conf.get("app.tables", fileConf.getString("app.tables"))
    val writeMode = if (conf.hasPath("app.writeMode")) conf.getString("app.writeMode") else "append"
    val slimUser  = if (conf.hasPath("user.slim")) conf.getBoolean("user.slim") else true

    // Small, general tunings (keep memory usage predictable on laptop)
    // Apply tunings from app.local.conf
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.shuffle.partitions", conf.getInt("tuning.shufflePartitions").toString)
    spark.conf.set("spark.sql.files.maxPartitionBytes", conf.getString("tuning.maxPartitionBytes"))
    spark.conf.set("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols",
      conf.getInt("delta.stats.numIndexedCols").toString)

    val maxRecsPerFile = if (conf.hasPath("writer.maxRecordsPerFile"))
      conf.getInt("writer.maxRecordsPerFile").toString else "50000"


    //  Determine tables to process: from command line arg or config
    val inputTables: Seq[String] =
      if (args.nonEmpty) args.head.split(",").map(_.trim).filter(_.nonEmpty).toSeq
      else tablesCsv.split(",").map(_.trim).filter(_.nonEmpty).toSeq

    // Import Spark SQL functions(all)
    import org.apache.spark.sql.functions._
    val hfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    try {
      // Process each table in one Spark session
      inputTables.foreach { tableName =>
        val inputPath = s"$srcDir/yelp_academic_dataset_${tableName}.json"
        val outputPath = s"$bronzeDir/$tableName"

        // Guard: skip if source file not found
        val exists = hfs.exists(new Path(inputPath))
        if (!exists) {
          System.err.println(s"[BronzeIngest][$tableName] SKIP: input not found -> $inputPath")
        } else {
          println(s"[BronzeIngest][$tableName] Reading  : $inputPath")

          // Read JSON with schema from Schemas object
          val raw = spark.read
            .schema(Schemas.getSchema(tableName))
            .json(inputPath)

          // Add ingestion metadata columns
          val withMeta = raw
            .withColumn("_ingest_ts", current_timestamp())
            .withColumn("_ingest_date", to_date(col("_ingest_ts")))
          // Print schema and sample rows for debuggind and verification
          withMeta.printSchema();

          println(s"[BronzeIngest][$tableName] Writing (Delta -> $outputPath) mode=$writeMode")

          // Partition by date; let Spark choose tasks (avoid hard-coded repartition(1))
          withMeta.write
            .mode(writeMode) // "append" for prod; "overwrite" for dev re-runs
            .format("delta")
            .option("maxRecordsPerFile", maxRecsPerFile)
            .partitionBy("_ingest_date")
            .save(outputPath)
        }
      }
      println("[BronzeIngest] All done.")
    } finally {
      // Close the Spark session
      spark.stop()
    }
  }
}