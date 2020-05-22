name := "SparkDataGenerator"

version := "0.1"

scalaVersion := "2.11.8"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.2"
/*
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-hdfs
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.1"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1"
*/


enablePlugins(PackPlugin)

val packSettings: Seq[Def.Setting[_]] = Seq(
  packMain := Map(),
  packGenerateWindowsBatFile := false,
  packGenerateMakefile := false
)