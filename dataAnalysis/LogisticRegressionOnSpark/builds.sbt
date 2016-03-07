lazy val common = Seq(
  organization := "week9.mids",
  version := "0.1.0",
  scalaVersion := "2.10.4",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.5.0",
    "org.elasticsearch" %% "elasticsearch-spark" % "2.2.0",
    "joda-time" % "joda-time" % "2.9.2",
    "org.joda" % "joda-convert" % "1.8.1",
    "org.apache.spark" %% "spark-mllib" % "1.5.0"
  ),
  mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
     {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
     }
  }
)

lazy val twitter_popularity = (project in file(".")).
  settings(common: _*).
  settings(
    name := "LogReg",
    mainClass in (Compile, run) := Some("LogReg.Main"))
