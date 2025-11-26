package com.yelpbatch.utils

// Imports
import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.sql.functions.{col, to_date}

object PostgreSQLWriter {

  // Logger
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  def getJDBCUrl(host: String, port: Int, database: String): String = {
    s"jdbc:postgresql://$host:$port/$database"
  }

  /** Utility to write DataFrame to PostgreSQL via JDBC
   * @param spark     : SparkSession
   * @param df        : DataFrame to write
   * @param jdbcUrl   : JDBC URL for PostgreSQL
   * @param tableName : target table name
   * @param user      : database user
   * @param password  : database password
   * @param enabled   : flag to enable/disable writing (default: true)
   * @param mode      : write mode (default: "append")
   * @param options   : additional JDBC options as key-value map
   * */
  def writeToPostgreSQL(
    spark: SparkSession,
    df: DataFrame,
    jdbcUrl: String,
    tableName: String,
    user: String,
    password: String,
    enabled: Boolean = true,
    mode: String = "append",
    options: Map[String, String] = Map.empty
  ): Unit = {

    // Check if writing is enabled
    if (!enabled) {
      logger.info("PostgreSQL writing is disabled. Skipping write operation.")
      return
    }

    logger.info(s"[PostgreSQLWriter] Writing to PostgreSQL table: $tableName")
    logger.info(s"[PostgreSQLWriter] JDBC URL: $jdbcUrl")
    logger.info(s"[PostgreSQLWriter] Mode: $mode")

    // Convert string date columns to proper DATE type
    val dfWithProperTypes: DataFrame = df
      .withColumn("day", to_date(col("day"))) // Convert string to DATE
      .withColumn("_gold_ingest_ts", col("_gold_ingest_ts").cast("timestamp"))

    // Write DataFrame to PostgreSQL via JDBC
    var writer = dfWithProperTypes.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .option("user", user)
      .option("password", password)
      .mode(mode)

    options.foreach { case (k, v) => writer = writer.option(k, v) }

    writer.save()

    logger.info(s"Data written to PostgreSQL table $tableName at $jdbcUrl")
  }

  /** Upsert DataFrame to PostgreSQL by deleting existing records before inserting
   * This handles duplicate key conflicts by removing existing data for the same keys
   *
   * @param spark      : SparkSession
   * @param df         : DataFrame to upsert
   * @param jdbcUrl    : JDBC URL for PostgreSQL
   * @param tableName  : target table name
   * @param user       : database user
   * @param password   : database password
   * @param deleteKeys : column names to use for identifying records to delete (e.g., Seq("day", "period_month"))
   * @param enabled    : flag to enable/disable writing (default: true)
   */
  def upsertToPostgreSQL(
    spark: SparkSession,
    df: DataFrame,
    jdbcUrl: String,
    tableName: String,
    user: String,
    password: String,
    deleteKeys: Seq[String],
    enabled: Boolean = true
  ): Unit = {

    if (!enabled) {
      logger.info("PostgreSQL writing is disabled. Skipping upsert operation.")
      return
    }

    import java.sql.DriverManager
    import spark.implicits._

    val rowCount = df.count()
    logger.info(s"[PostgreSQLWriter] Upserting $rowCount rows to PostgreSQL table: $tableName")
    logger.info(s"[PostgreSQLWriter] Delete keys: ${deleteKeys.mkString(", ")}")

    // Convert string date columns to proper DATE type
    val dfWithProperTypes: DataFrame = df
      .withColumn("day", to_date(col("day")))
      .withColumn("_gold_ingest_ts", col("_gold_ingest_ts").cast("timestamp"))

    // Step 1: Get distinct values for delete keys
    val keysToDelete = dfWithProperTypes
      .select(deleteKeys.map(col): _*)
      .distinct()
      .collect()

    if (keysToDelete.isEmpty) {
      logger.warn(s"[PostgreSQLWriter] No data to upsert to $tableName")
      return
    }

    // Step 2: Build DELETE statement
    val deleteConditions = keysToDelete.map { row =>
      deleteKeys.zipWithIndex.map { case (colName, idx) =>
        val value = row.get(idx)
        val formattedValue = value match {
          case s: String => s"'$s'"
          case d: java.sql.Date => s"'$d'"
          case n: Number => n.toString
          case null => "NULL"
          case other => s"'$other'"
        }
        s"$colName = $formattedValue"
      }.mkString(" AND ")
    }.mkString(" OR ")

    val deleteSQL = s"DELETE FROM $tableName WHERE $deleteConditions"

    logger.info(s"[PostgreSQLWriter] Deleting existing records from $tableName")
    logger.debug(s"[PostgreSQLWriter] DELETE SQL: $deleteSQL")

    // Step 3: Execute DELETE
    var connection: java.sql.Connection = null
    try {
      Class.forName("org.postgresql.Driver")
      connection = DriverManager.getConnection(jdbcUrl, user, password)
      val statement = connection.createStatement()
      val deletedRows = statement.executeUpdate(deleteSQL)
      logger.info(s"[PostgreSQLWriter] Deleted $deletedRows existing rows from $tableName")
      statement.close()
    } catch {
      case e: Exception =>
        logger.error(s"[PostgreSQLWriter] Failed to delete existing records from $tableName", e)
        throw e
    } finally {
      if (connection != null && !connection.isClosed) {
        connection.close()
      }
    }

    // Step 4: Insert new data
    logger.info(s"[PostgreSQLWriter] Inserting $rowCount new rows into $tableName")
    dfWithProperTypes.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .option("user", user)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .option("batchsize", "1000")
      .option("isolationLevel", "READ_COMMITTED")
      .mode("append")
      .save()

    logger.info(s"[PostgreSQLWriter] Successfully upserted $rowCount rows to $tableName")
  }
}