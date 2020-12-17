name := "miro-thuiswerk"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.12.10"
val sparkVersion = "3.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % sparkVersion, //% Provided,
  "org.apache.spark" % "spark-sql_2.12" % sparkVersion, //% Provided,
  "org.apache.spark" % "spark-hive_2.12" % sparkVersion, //% Provided,
  "com.github.scopt" %% "scopt" % "4.0.0" % Compile,
  "org.scalatest" %% "scalatest" % "3.2.3" % "test, it"
)

assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
}

// test run settings
//parallelExecution in Test := false
assembly / test := {}

// Enable integration tests
Defaults.itSettings
lazy val root = project.in(file(".")).configs(IntegrationTest)

// Measure time for each test
Test / testOptions += Tests.Argument("-oD")
IntegrationTest / testOptions += Tests.Argument("-oD")

// Scoverage settings
coverageExcludedPackages := "<empty>;.*storage.*"
coverageMinimum := 70
coverageFailOnMinimum := true

// Scalastyle settings
scalastyleFailOnWarning := false
scalastyleFailOnError := true
