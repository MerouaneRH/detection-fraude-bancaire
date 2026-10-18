ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0"

val sparkVersion = "3.5.1"

// Spark 3.5 sur Java 17/21 : ouvrir les modules internes du JDK
val sparkJvmOptions = Seq(
  "-Xmx3g",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "detection-fraude-bancaire",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided,
      "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,
      "org.scalatest"    %% "scalatest"  % "3.2.18"     % Test
    ),
    // Spark en local pour sbt run / sbt test (Provided doit rester sur le classpath de run)
    Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated,
    Compile / runMain := Defaults.runMainTask(Compile / fullClasspath, Compile / run / runner).evaluated,
    Compile / run / fork := true,
    Compile / run / javaOptions ++= sparkJvmOptions,
    Test / fork := true,
    Test / javaOptions ++= sparkJvmOptions
  )
