course := "reactive"
assignment := "kvstore"

testFrameworks += new TestFramework("munit.Framework")
Test / parallelExecution := false

val akkaVersion = "2.6.9"

scalaVersion := "2.13.5"
scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-encoding", "UTF-8",
  "-unchecked",
  "-Xlint",
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  "org.scalameta" %% "munit" % "0.7.22" % Test
)
