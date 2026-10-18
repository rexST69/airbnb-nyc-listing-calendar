ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "airbnb-nyc-listing-calendar",
    version := "0.1.0",
    Compile / scalaSource := baseDirectory.value / "scripts/src/main/scala",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided",
      "org.apache.spark" %% "spark-mllib" % "3.5.1" % "provided"
    ),
    assembly / mainClass := Some("ingestion.AirbnbDataIngestion"),
    assembly / assemblyJarName := "airbnb-nyc-listing-calendar.jar"
  )