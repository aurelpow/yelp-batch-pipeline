ThisBuild / organization := "com.yelpbatch"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.20"

val sparkV = "3.5.1"

lazy val root = (project in file("."))
  .settings(
    name := "yelp-batch-project",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkV % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkV % "provided",
      "io.delta" %% "delta-spark" % "3.3.1",
      "com.typesafe" % "config" % "1.4.3",
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
    )
  )