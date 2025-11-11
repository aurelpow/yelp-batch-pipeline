package com.yelpbatch.app

import com.typesafe.config.{Config, ConfigFactory}

case class PathsConfig(
                        rawDir: String,
                        bronzeDir: String,
                        silverDir: String,
                        goldDir: String
                      )

case class MongoConfig(
                        enabled: Boolean,
                        uri: String,
                        database: String
                      )

case class AppConfig(
                      paths: PathsConfig,
                      mongodb: MongoConfig
                    )

object AppConfig {
  /**
   * Load environment-specific configuration (raw Config)
   *
   * @param env Environment name: "dev", "local", or "prod"
   * @return Resolved Config object
   */
  def load(env: String): Config = {
    val envConfig = env.toLowerCase match {
      case "dev"   => ConfigFactory.load("dev.conf")
      case "local" => ConfigFactory.load("local.conf")
      case "prod"  => ConfigFactory.load("prod.conf")
      case _       => ConfigFactory.load() // fallback to application.conf
    }
    envConfig.resolve()
  }

  /**
   * Load typed AppConfig from environment
   *
   * @param env Environment name
   * @return Typed AppConfig case class
   */
  def loadTyped(env: String): AppConfig = {
    val config = load(env)
    AppConfig(
      paths = PathsConfig(
        rawDir = config.getString("paths.rawDir"),
        bronzeDir = config.getString("paths.bronzeDir"),
        silverDir = config.getString("paths.silverDir"),
        goldDir = config.getString("paths.goldDir")
      ),
      mongodb = MongoConfig(
        enabled = config.getBoolean("mongodb.enabled"),
        uri = config.getString("mongodb.uri"),
        database = config.getString("mongodb.database")
      )
    )
  }
}
