name := "schemaroller"

version := "0.1.0"

scalaVersion := "2.12.12"

val sparkVersion = "2.4.0"

// Spark Dependencies
libraryDependencies ++= Seq(
  "spark-core",
  "spark-sql",
).map("org.apache.spark" %% _ % sparkVersion)

// Test Dependencies
libraryDependencies ++= Seq(
  "org.scalactic" %% "scalactic" % "3.1.0",
  "org.scalamock" %% "scalamock" % "4.4.0",
  "org.scalatest" %% "scalatest" % "3.1.0"
).map(_ % "test")

assemblyJarName in assembly := "schemaroller.jar"

assemblyMergeStrategy in assembly := {
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", xs @ _*) =>
    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first
    }
  case PathList("org", "apache", "hadoop", "yarn", "factories", "package-info.class") => MergeStrategy.discard
  case PathList("org", "apache", "hadoop", "yarn", "provider", "package-info.class") => MergeStrategy.discard
  case PathList("org", "apache", "hadoop", "util", "provider", "package-info.class") => MergeStrategy.discard
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
  case PathList("org", "apache", "arrow", "git.properties") => MergeStrategy.discard
  case _ => MergeStrategy.first
}