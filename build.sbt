name := "hyoztyur"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion/* % "provided"*/,
  "org.apache.spark" %% "spark-sql" % sparkVersion/* % "provided"*/
)

lazy val commonSettings = Seq(
  version := "1.0-SNAPSHOT",
  organization := "com.example",
  scalaVersion := sparkVersion,
  test in assembly := {}
)

lazy val app = (project in file("app")).settings(commonSettings: _*).settings(mainClass in assembly := Some("com.example.Main"))

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _ *) => MergeStrategy.discard

  case x =>
    MergeStrategy.first
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "hyoztyur.jar"

fullClasspath in Runtime := (fullClasspath in (Compile, run)).value
