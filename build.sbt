name := "testsparkjob"

version := "1.0"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

scalaVersion := "2.11.11"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.10
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.1"
libraryDependencies += "datastax" % "spark-cassandra-connector" % "2.0.1-s_2.11"