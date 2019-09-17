# Spark_With_Scala

RDDs,SparkSQL,SparkML

 https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.11
 
  spark-shell --driver-class-path mysql-connector-java-8.0.11.jar
  import org.apache.spark.sql.SQLContext

  val url = "jdbc:mysql://localhost:3306/sparktest?user=ash&password=*******"

  sqlContext.load("jdbc", Map("url" -> url,"dbtable" -> "orders")).toDF.show
