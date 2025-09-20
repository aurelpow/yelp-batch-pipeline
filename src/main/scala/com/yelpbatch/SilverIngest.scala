package com.yelpbatch
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession


object SilverIngest {

  def main(args: Array[String]): Unit = {

    // Initialize Spark session with application name for cluster monitoring
    val spark = SparkSession.builder
      .appName("SilverIngest")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    // Set Spark log level to "ERROR" to suppress INFO and WARN output
    spark.sparkContext.setLogLevel("ERROR")

    // Load defaults from classpath resource (app.local.conf)
    val fileConf: Config =
      ConfigFactory.parseResources("app.local.conf").resolve()

    // --- Config (simple & safe defaults) ---
    val conf = ConfigFactory.parseResources("app.local.conf").resolve()
    val srcDir = conf.getString("paths.src")
    val bronzeDir = conf.getString("paths.bronzeDir")
    val silverDir = conf.getString("paths.silverDir")
    val tablesCsv = spark.conf.get("app.tables", fileConf.getString("app.tables"))
    val writeMode = if (conf.hasPath("app.writeMode")) conf.getString("app.writeMode") else "append"
    val slimUser = if (conf.hasPath("user.slim")) conf.getBoolean("user.slim") else true

  }
}
