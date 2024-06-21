ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"
lazy val root = (project in file("."))
  .settings(
    name := "spark-scala-project"
  )
val sparkVersion = "3.5.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.2.18" % Test,
  "org.scalatest" %% "scalatest-funsuite" % "3.2.18" % Test
)



