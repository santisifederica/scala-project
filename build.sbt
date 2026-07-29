name := "EarthquakeCooccurrence"

version := "1.0"

scalaVersion := "2.12.18"

//Versione per Google Cloud Dataproc
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql"  % "3.3.2" % "provided"

//Versione per esecuzione in locale
//libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2"
//libraryDependencies += "org.apache.spark" %% "spark-sql"  % "3.3.2"

assembly / assemblyJarName := "earthquake-assembly.jar"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}