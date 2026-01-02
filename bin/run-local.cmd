@echo off
set JAR=target/scala-2.12/yelp-batch-project-assembly-0.1.0-SNAPSHOT.jar
set POSTGRES_JAR=%~dp0..\lib\postgresql-42.7.8.jar
set LOG4J_CONF=%~dp0..\conf\log4j2.properties
set LOG4J_CONF_URL=file:///%LOG4J_CONF:\=/%

spark-submit ^
  --master local[4] ^
  --driver-memory 4g ^
  --jars %POSTGRES_JAR% ^
  --driver-class-path %POSTGRES_JAR% ^
  --conf spark.executor.extraClassPath=%POSTGRES_JAR% ^
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=%LOG4J_CONF_URL%" ^
  --class com.yelpbatch.app.Runner ^
  %JAR% %*