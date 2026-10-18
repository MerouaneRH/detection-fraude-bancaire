ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0"

val sparkVersion = "3.5.1"

lazy val root = (project in file("."))
  .settings(
    name := "detection-fraude-bancaire",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided,
      "org.scalatest"    %% "scalatest"  % "3.2.18"     % Test
    ),
    // Spark en local pour sbt run / sbt test
    Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated,
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "-Xmx2g",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
  )
