name := "lorgnette"

version := "0.1"

scalaVersion := "2.12.12"

// Spark Dependencies
val sparkVersion = "2.4.0"
libraryDependencies ++= Seq(
  "spark-core",
  "spark-sql"
).map("org.apache.spark" %% _ % sparkVersion)

// Test Dependencies
libraryDependencies ++= Seq(
  "org.scalamock" %% "scalamock" % "4.4.0",
  "org.scalactic" %% "scalactic" % "3.1.0",
  "org.scalatest" %% "scalatest" % "3.1.0"
).map(_ % Test)
