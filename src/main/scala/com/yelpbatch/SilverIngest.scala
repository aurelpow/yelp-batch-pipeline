package com.yelpbatch
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable

/**
 * SilverIngest - Spark application for transforming Bronze layer data into clean Silver fact tables
 * 
 * This application:
 * 1. Reads data from Bronze layer (Delta tables)
 * 2. Applies data quality rules and transformations
 * 3. Creates clean fact tables in Silver layer
 * 4. Uses snapshot tables for point-in-time data consistency
 * 5. Implements proper merge/upsert logic for data freshness
 */
object SilverIngest {

  def main(args: Array[String]): Unit = {

    // Initialize Spark session with application name for cluster monitoring
    val spark = SparkSession.builder
      .appName("SilverIngest")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    
    // For Spark 3.0+, set legacy time parser policy to LEGACY for backward compatibility
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    // Set Spark log level to "ERROR" to suppress INFO and WARN output
    spark.sparkContext.setLogLevel("ERROR")

    // Load defaults from classpath resource (app.local.conf)
    val fileConf: Config = ConfigFactory.parseResources("app.local.conf").resolve()

    // --- Config (simple & safe defaults) ---
    val conf = ConfigFactory.parseResources("app.local.conf").resolve()
    val bronzeDir = conf.getString("paths.bronzeDir")
    val silverDir = conf.getString("paths.silverDir")
    val tablesCsv = spark.conf.get("app.tables", fileConf.getString("app.tables"))
    val writeMode = if (conf.hasPath("app.writeMode")) conf.getString("app.writeMode") else "append"

    // Apply tunings from app.local.conf
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.shuffle.partitions", conf.getInt("tuning.shufflePartitions").toString)
    spark.conf.set("spark.sql.files.maxPartitionBytes", conf.getString("tuning.maxPartitionBytes"))
    spark.conf.set("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols",
      conf.getInt("delta.stats.numIndexedCols").toString)

    val maxRecsPerFile = if (conf.hasPath("writer.maxRecordsPerFile"))
      conf.getInt("writer.maxRecordsPerFile").toString else "50000"

    // Determine tables to process: from command line arg or config
    val inputTables: Seq[String] =
      if (args.nonEmpty) args.head.split(",").map(_.trim).filter(_.nonEmpty).toSeq
      else tablesCsv.split(",").map(_.trim).filter(_.nonEmpty).toSeq

    val hfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    try {
      println(s"[SilverIngest] Starting processing for tables: ${inputTables.mkString(", ")}")
      
      // Process each table in one Spark session
      inputTables.foreach { tableName =>
        processTable(spark, hfs, bronzeDir, silverDir, tableName, maxRecsPerFile, writeMode)
      }
      
      println("[SilverIngest] All tables processed successfully.")
    } finally {
      // Close the Spark session
      spark.stop()
    }
  }

  /**
   * Process a single table from Bronze to Silver layer
   */
  def processTable(
    spark: SparkSession,
    hfs: FileSystem,
    bronzeDir: String,
    silverDir: String,
    tableName: String,
    maxRecsPerFile: String,
    writeMode: String
  ): Unit = {
    
    val inputPath = s"$bronzeDir/$tableName"
    val outputPath = s"$silverDir/${tableName}_snapshot"

    // Check if bronze table exists
    if (!hfs.exists(new Path(inputPath))) {
      System.err.println(s"[SilverIngest][$tableName] SKIP: bronze table not found -> $inputPath")
      return
    }

    println(s"[SilverIngest][$tableName] Processing bronze table: $inputPath")

    // Refresh Delta table metadata to ensure consistency
    try {
      spark.sql(s"REFRESH TABLE delta.`$inputPath`")
      println(s"[SilverIngest][$tableName] Refreshed Delta table metadata")
    } catch {
      case e: Exception =>
        println(s"[SilverIngest][$tableName] WARN: Could not refresh table metadata: ${e.getMessage}")
      // Continue anyway - the read might still work
    }

    // Read bronze table (get latest data)
    val bronzeTable = spark.read
      .format("delta")
      .load(inputPath)

    // Apply transformations based on table type
    val silverTable = tableName.toLowerCase match {
      case "business" => transformBusinessTable(bronzeTable)
      case "user" => transformUserTable(bronzeTable)
      case "review" => transformReviewTable(bronzeTable)
      case "checkin" => transformCheckinTable(bronzeTable)
      case "tip" => transformTipTable(bronzeTable)
      case _ =>
        System.err.println(s"[SilverIngest][$tableName] WARN: No transformation rules defined, copying as-is")
        bronzeTable
    }

    // Add metadata columns for Silver layer
    val silverWithMeta = silverTable
      .withColumn("_silver_ingest_ts", current_timestamp())

    // Check schema and sample data
    silverWithMeta.printSchema()
    silverWithMeta.show(5, truncate = false)

    // Write to Silver layer with proper merge logic
    writeSilverTable(spark, silverWithMeta, outputPath, maxRecsPerFile, writeMode, tableName)
  }

  /**
   * Transform business table with data quality rules
   */
  def transformBusinessTable(bronzeTable: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    val columnsToCast = Seq(
      "stars" -> "integer",
      "review_count" -> "integer",
      "is_open" -> "boolean",
      "latitude" -> "double",
      "longitude" -> "double"
    )

    val base = bronzeTable
      .filter(col("business_id").isNotNull && col("business_id") =!= "")
      .withColumn("name", trim(col("name")))
      .withColumn("city", trim(initcap(col("city"))))
      .withColumn("state", upper(trim(col("state"))))
      .withColumn("categories", trim(col("categories")))
      // Data quality rules
      .withColumn("stars", when(col("stars") < 1 || col("stars") > 5, null).otherwise(col("stars")))
      .withColumn("review_count", when(col("review_count") < 0, 0).otherwise(col("review_count")))
      .withColumn("latitude", when(col("latitude") < -90 || col("latitude") > 90, null).otherwise(col("latitude")))
      .withColumn("longitude", when(col("longitude") < -180 || col("longitude") > 180, null).otherwise(col("longitude")))

    val dfClean = columnsToCast.foldLeft(base) { case (df, (name, typ)) =>
      df.withColumn(name, col(name).cast(typ))
    }

    // Select final columns for business fact table
    dfClean.select(
      "business_id",
      "name", 
      "city",
      "state",
      "latitude",
      "longitude",
      "stars",
      "review_count",
      "is_open",
      "categories",
      "_ingest_ts"
    )
  }

  /**
   * Transform user table with data quality rules
   */
  def transformUserTable(bronzeTable: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    bronzeTable
      .filter(col("user_id").isNotNull && col("user_id") =!= "")
      .withColumn("name", trim(col("name")))
      .withColumn("yelping_since", to_date(col("yelping_since"), "yyyy-MM-dd"))
      .withColumn("friends", when(col("friends").isNull || col("friends") === "None", "").otherwise(col("friends")))
      .withColumn("friend_count", 
        when(col("friends") === "" || col("friends").isNull, 0)
        .otherwise(size(split(col("friends"), ","))))
      // Data quality rules
      .withColumn("review_count", when(col("review_count") < 0, 0).otherwise(col("review_count")))
      .withColumn("fans", when(col("fans") < 0, 0).otherwise(col("fans")))
      .withColumn("average_stars", 
        when(col("average_stars") < 1.0 || col("average_stars") > 5.0, null)
        .otherwise(col("average_stars")))
      .withColumn("useful", when(col("useful") < 0, 0).otherwise(col("useful")))
      .withColumn("funny", when(col("funny") < 0, 0).otherwise(col("funny")))
      .withColumn("cool", when(col("cool") < 0, 0).otherwise(col("cool")))
      .drop("friends") // Remove raw friends field, keep only count
      .select(
        "user_id",
        "name",
        "yelping_since",
        "review_count",
        "fans",
        "average_stars",
        "useful",
        "funny",
        "cool",
        "friend_count",
        "_ingest_ts"
      )
  }

  /**
   * Transform review table with data quality rules
   */
  def transformReviewTable(bronzeTable: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    bronzeTable
      .filter(
        col("review_id").isNotNull && col("review_id") =!= "" &&
        col("user_id").isNotNull && col("user_id") =!= "" &&
        col("business_id").isNotNull && col("business_id") =!= ""
      )
      .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
      .withColumn("stars", 
        when(col("stars") < 1 || col("stars") > 5, null)
        .otherwise(col("stars").cast("integer")))
      .withColumn("text", trim(col("text")))
      // Data quality rules
      .withColumn("useful", when(col("useful") < 0, 0).otherwise(col("useful")))
      .withColumn("funny", when(col("funny") < 0, 0).otherwise(col("funny")))
      .withColumn("cool", when(col("cool") < 0, 0).otherwise(col("cool")))
      .select(
        "review_id",
        "user_id",
        "business_id",
        "stars",
        "useful",
        "funny",
        "cool",
        "date",
        "text",
        "_ingest_ts"
      )
  }

  /**
   * Transform checkin table with data quality rules
   */
  def transformCheckinTable(bronzeTable: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    bronzeTable
      .filter(col("business_id").isNotNull && col("business_id") =!= "")
      .withColumn("date", explode(split(col("date"), ",\\s*")))
      .filter(col("date") =!= "" && col("date").isNotNull)
      .withColumn("checkin_date", 
        when(col("date").contains(" "), 
          to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
        .otherwise(
          to_timestamp(concat(col("date"), lit(" 00:00:00")), "yyyy-MM-dd HH:mm:ss")))
      .filter(col("checkin_date").isNotNull)
      .drop("date")
      .select(
        "business_id",
        "checkin_date",
        "_ingest_ts"
      )
  }

  /**
   * Transform tip table with data quality rules
   */
  def transformTipTable(bronzeTable: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    bronzeTable
      .filter(
        col("user_id").isNotNull && col("user_id") =!= "" &&
        col("business_id").isNotNull && col("business_id") =!= ""
      )
      .withColumn("text", trim(col("text")))
      .withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("compliment_count", 
        when(col("compliment_count") < 0, 0)
        .otherwise(col("compliment_count").cast("integer")))
      .select(
        "user_id",
        "business_id",
        "text",
        "date",
        "compliment_count",
        "_ingest_ts"
      )
  }

  /**
   * Write Silver table with proper merge/upsert logic
   */
  def writeSilverTable(
    spark: SparkSession,
    silverTable: org.apache.spark.sql.DataFrame,
    outputPath: String,
    maxRecsPerFile: String,
    writeMode: String,
    tableName: String
  ): Unit = {
    
    val hfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    
    println(s"[SilverIngest][$tableName] Writing to: $outputPath")

    try {
      if (hfs.exists(new Path(outputPath))) {
        // Table exists - use merge logic for upserts
        val deltaTable = DeltaTable.forPath(outputPath)
        
        // Get the primary key column for each table
        val primaryKey = tableName.toLowerCase match {
          case "business" => "business_id"
          case "user" => "user_id"
          case "review" => "review_id"
          case "checkin" => col("business_id") && col("checkin_date") // Composite key
          case "tip" => col("user_id") && col("business_id") && col("date") // Composite key
          case _ => "id"
        }

        if (tableName.toLowerCase == "checkin" || tableName.toLowerCase == "tip") {
          // For tables with composite keys, use append mode
          silverTable.write
            .mode("append")
            .format("delta")
            .option("maxRecordsPerFile", maxRecsPerFile)
            .save(outputPath)
        } else {
          // For tables with single primary key, use merge
          deltaTable.as("target")
            .merge(silverTable.as("source"), s"target.$primaryKey = source.$primaryKey")
            .whenMatched()
            .updateAll()
            .whenNotMatched()
            .insertAll()
            .execute()
        }
      } else {
        // Table doesn't exist - create it
        silverTable.write
          .mode("overwrite")
          .format("delta")
          .option("maxRecordsPerFile", maxRecsPerFile)
          .save(outputPath)
      }
      
      println(s"[SilverIngest][$tableName] Successfully wrote ${silverTable.count()} records to $outputPath")
      
    } catch {
      case e: Exception =>
        System.err.println(s"[SilverIngest][$tableName] ERROR writing to $outputPath: ${e.getMessage}")
        throw e
    }
  }
}