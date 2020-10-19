name := "sparkRealApp"

version := "0.1"

scalaVersion := "2.12.4"
val sparkVersion = "3.0.1"
val catsVersion = "2.0.0"

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % catsVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
