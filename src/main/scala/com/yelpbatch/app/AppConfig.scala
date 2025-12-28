package com.yelpbatch.app

import com.typesafe.config.{Config, ConfigFactory}
import pureconfig._
import pureconfig.generic.auto._

case class BronzePaths(
                          review: String,
                          tip: String,
                          business: String,
                          checkin: String,
                          user: String
                        )

case class SilverPaths(
                          review: String,
                          tip: String,
                          business: String,
                          checkin: String,
                          user: String
                        )

case class GoldPaths(
                      factReviewTipMetrics: String,
                      businessPopularity: String
                    )

case class PathsConfig(
                        rawDir: String,
                        bronzeDir: String,
                        silverDir: String,
                        goldDir: String,
                        bronze: BronzePaths,
                        silver: SilverPaths,
                        gold: GoldPaths
                      )

case class MongoConfig(
                        enabled: Boolean,
                        uri: Option[String],
                        database: Option[String]
                      )

case class PostgresConfig(
                           enabled: Boolean,
                           host: Option[String],
                           port: Option[Int],
                           database: Option[String],
                           user: Option[String],
                           password: Option[String]
                         )

case class GranularityConfig(
                              daily: Int,
                              monthly: Int
                            )

case class OptimizeConfig(
                           partition: String,
                           zorder: List[String]
                         )

case class TuningConfig(
                         legacyTimeParserPolicy: String,
                         executorHeartbeat: String,
                         shufflePartitions: Int,
                         maxPartitionBytes: String,
                         sourcesPartitionOverwriteMode: String,
                         partitionOverwriteMode: String,
                         log4jpath: String
                       )

case class StatsConfig(
                        numIndexedCols: Int
                      )

case class DeltaConfig(
                        stats: StatsConfig
                      )

case class WriterConfig(
                         maxRecordsPerFile: Int
                       )

case class SparkConfig(
                        deployMode: String
                      )

case class AppSectionConfig(
                             tables: String,
                             writeMode: String
                           )

case class AppConfig(
                      paths: PathsConfig,
                      mongodb: MongoConfig,
                      postgresql: PostgresConfig,
                      granularity: GranularityConfig,
                      optimize: OptimizeConfig,
                      tuning: TuningConfig,
                      delta: DeltaConfig,
                      writer: WriterConfig,
                      spark: SparkConfig,
                      app: AppSectionConfig
                    )

object AppConfig {
  import pureconfig.generic.ProductHint
  import pureconfig.ConfigFieldMapping
  import pureconfig.CamelCase
  import pureconfig.KebabCase

  // Configure PureConfig to use CamelCase matching (e.g. legacyTimeParserPolicy matches legacyTimeParserPolicy)
  // But also handle kebab-case for keys like "delta-stats" if they were kebab-case, but here they are dot-separated in HOCON which maps to nested objects usually.
  // However, "delta.stats" in HOCON is a nested object "stats" inside "delta".
  // My Case Class has `deltaStats`.
  // I need to map `deltaStats` to `delta.stats`.
  // PureConfig default is KebabCase (delta-stats).
  // Let's use a custom mapping or adjust the case class.
  // Easier to adjust case class to match structure or use FieldCoproductHint? No.

  // Actually, "delta.stats" in HOCON means:
  // delta {
  //   stats { ... }
  // }
  // So I should have a DeltaConfig case class with a stats field.
  // OR I can use @ConfigFieldMapping to map "deltaStats" to "delta.stats" (which is not a standard field name, it's a path).

  // Let's simplify: I will change the case class structure to match HOCON nesting for delta.stats.

  implicit def hint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

  /**
   * Load environment-specific configuration (raw Config) without validation
   */
  private def loadRaw(env: String): Config = {
    val envConfig = env.toLowerCase match {
      case "dev" => ConfigFactory.load("dev.conf")
      case "local" => ConfigFactory.load("local.conf")
      case "prod" => ConfigFactory.load("prod.conf")
      case _ => ConfigFactory.load() // fallback to application.conf
    }
    envConfig.resolve()
  }

  /**
   * Load and validate configuration
   * @return Resolved Config object
   */
  def load(env: String): Config = {
    val config = loadRaw(env)
    validate(config)
    config
  }

  private def validate(config: Config): Unit = {
    ConfigSource.fromConfig(config).load[AppConfig] match {
      case Right(_) =>
        println("✓ Configuration validated successfully")
      case Left(failures) =>
        throw new RuntimeException(s"Configuration validation failed:\n${failures.toList.map(_.description).mkString("\n")}")
    }
  }

  /**
   * Load both raw and typed AppConfig
   *
   * @param env Environment name
   * @return (Config, AppConfig) tuple
   */
  def loadTyped(env: String): (Config, AppConfig) = {
    val config = loadRaw(env)
    validate(config)
    val typed = ConfigSource.fromConfig(config).loadOrThrow[AppConfig]
    (config, typed)
  }
}
