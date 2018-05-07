
name := "HTWG KI Seminar 2018 - Poker Prediction"
version := "0.1"
scalaVersion := "2.11.12"
//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0-SNAPSHOT"
//resolvers += "Local Maven Repository" at "file://C://Users//kim.desouza//.m2//repository"

val sparkVersion = "2.3.0"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "mysql" % "mysql-connector-java" % "5.1.6"
)