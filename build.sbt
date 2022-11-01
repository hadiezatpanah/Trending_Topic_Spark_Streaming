
name := "TOPIC_TREND"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

val SparkCompatibleVersion = "2.4"

val ScalaCompatibleVersion = "2.11"

val dependencyScope = "compile"

// adding some resolver for packages
resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",
  "org.ini4j" % "ini4j" % "0.5.4",
  "org.postgresql" % "postgresql" % "42.4.1"
//  "io.delta" %% "delta-core" % "0.6.1" % "provided"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


assemblyJarName in assembly := s"${name.value}-${version.value}.jar"
publishArtifact in makePom := false
publishArtifact in (Compile, packageBin) := false
publishArtifact in (Compile, packageDoc) := false
publishArtifact in (Compile, packageSrc) := false
publishConfiguration := publishConfiguration.value.withOverwrite(true)

assembly / artifact := {
  val art = (assembly / artifact).value
  art.withClassifier(Some("assembly"))
}

addArtifact(assembly / artifact, assembly)

publishTo := Some(Resolver.file("file", new File("./spark")))
