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
      // "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
      // "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
      // "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion,
      "io.delta" %% "delta-spark" % "3.2.0",
      "com.typesafe" % "config" % "1.4.3",
      "org.scalatest" %% "scalatest" % "3.2.18" % Test,
      "org.mongodb.spark" %% "mongo-spark-connector" % "10.4.0",
      "org.mongodb" % "mongodb-driver-sync" % "5.2.1",  // Add explicit driver version
    ),

    // Assembly settings
    assembly / assemblyJarName := s"${name.value}-assembly-${version.value}.jar",
    assembly / mainClass := Some("com.yelpbatch.BronzeIngest"),  // Changed from Runner

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

    // Critical: Exclude Spark classes from assembly
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp.filter { jar =>
        val name = jar.data.getName
        name.startsWith("spark-") ||
          name.startsWith("scala-library") ||
          name.startsWith("scala-reflect")
      }
    }
  )