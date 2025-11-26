ThisBuild / organization := "com.yelpbatch"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.20"

val sparkV = "3.5.1"
val log4jVersion = "2.17.2"

lazy val root = (project in file("."))
  .settings(
    name := "yelp-batch-project",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkV % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkV % "provided",
      "io.delta" %% "delta-spark" % "3.2.0",
      "com.typesafe" % "config" % "1.4.3",
      "org.scalatest" %% "scalatest" % "3.2.18" % Test,
      "org.mongodb.spark" %% "mongo-spark-connector" % "10.4.0",
      "org.mongodb" % "mongodb-driver-sync" % "5.2.1",
      "org.postgresql" % "postgresql" % "42.7.8"
    ),

    // Assembly settings
    assembly / assemblyJarName := s"${name.value}-assembly-${version.value}.jar",
    assembly / mainClass := Some("com.yelpbatch.app.Runner"),
    assembly / assemblyPrependShellScript := None,

    // Merge strategy
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "native-image", xs @ _*) => MergeStrategy.first
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case "application.conf" => MergeStrategy.concat
      case "log4j.properties" => MergeStrategy.first
      case x if x.endsWith(".proto") => MergeStrategy.first
      case _ => MergeStrategy.first
    },

    // Exclude Spark runtime JARs (provided by cluster in production)
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp.filter { jar =>
        val name = jar.data.getName.toLowerCase
        name.startsWith("spark-") ||
          name.contains("hadoop-") ||
          name.contains("scala-library") ||
          name.contains("scala-reflect")
      }
    }
  )