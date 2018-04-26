name := "cse8803_hw4"

version := "1.0"

scalaVersion := "2.10.5"

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

evictionWarningOptions in update :=
  EvictionWarningOptions.default
    .withWarnTransitiveEvictions(false)
    .withWarnDirectEvictions(false)
    .withWarnScalaVersionEviction(false)


libraryDependencies ++= Seq(
  "org.apache.spark"  % "spark-core_2.10"              % "1.3.1" % "provided",
  "org.apache.spark"  % "spark-mllib_2.10"             % "1.3.1",
  "org.apache.spark"  % "spark-graphx_2.10"            % "1.3.1",
  "com.databricks"    % "spark-csv_2.10"               % "1.3.0"
)

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test"

parallelExecution in Test := false
