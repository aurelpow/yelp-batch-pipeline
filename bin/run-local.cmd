@echo off
set JAR=target/scala-2.12/yelp-batch-project-assembly-0.1.0-SNAPSHOT.jar
set POSTGRES_JAR=%~dp0..\lib\postgresql-42.7.8.jar

spark-submit ^
  --master local[4] ^
  --driver-memory 4g ^
  --jars %POSTGRES_JAR% ^
  --driver-class-path %POSTGRES_JAR% ^
  --conf spark.executor.extraClassPath=%POSTGRES_JAR% ^
  --class com.yelpbatch.app.Runner ^
  %JAR% %*