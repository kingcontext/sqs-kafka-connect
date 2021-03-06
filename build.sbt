import sbt._
import sbtassembly.AssemblyKeys
import scala.language.postfixOps

enablePlugins(GitVersioning)
git.useGitDescribe := true

name := "sqs-kafka-connect"
organization := "com.hivehome"

scalaVersion in ThisBuild := "2.11.8"
crossScalaVersions := Seq("2.11.8", "2.12.0")

ivyScala := ivyScala.value map {
  _.copy(overrideScalaVersion = true)
}

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
updateOptions := updateOptions.value.withCachedResolution(true)

val jarRepoReleases = SettingKey[String]("jarRepoReleases", "JAR repository address for releases.")
val jarRepoSnapshots = SettingKey[String]("jarRepoSnapshots", "JAR repository address for snapshots.")

jarRepoReleases := util.Properties.envOrElse("DP_JAR_REPO_RELEASES", "https://bgchops.artifactoryonline.com/bgchops/dataplatform-maven-releases")
jarRepoSnapshots := util.Properties.envOrElse("DP_JAR_REPO_SNAPSHOTS", "https://bgchops.artifactoryonline.com/bgchops/dataplatform-maven-snapshots")

resolvers in ThisBuild ++= Seq(
  "DP Nexus Snapshots" at s"${jarRepoSnapshots.value}",
  "DP Nexus Releases" at s"${jarRepoReleases.value}",
  "Confluent" at "http://packages.confluent.io/maven/"
)

publishTo in ThisBuild := {
  if (Path.userHome / ".ivy2" / ".credentials" exists)
    Credentials.add(Path.userHome / ".ivy2" / ".credentials", sbt.ConsoleLogger())
  if (isSnapshot.value) Some("snapshots" at jarRepoSnapshots.value)
  else Some("releases" at jarRepoReleases.value)
}

lazy val dependencies = Seq(
  "com.amazonaws" % "amazon-sqs-java-messaging-lib" % Versions.AwsSqsJms,
  "org.apache.avro" % "avro" % Versions.Avro % Provided,
  "org.apache.kafka" % "kafka-clients" % Versions.Kafka % Provided,
  "org.apache.kafka" % "connect-api" % Versions.Kafka % Provided,
  "io.confluent" % "kafka-avro-serializer" % Versions.Confluent % Provided,
  "io.confluent" % "kafka-schema-registry-client" % Versions.Confluent % Provided ,
  "io.confluent" % "kafka-connect-avro-converter" % Versions.Confluent % Provided,
  "org.slf4j" % "slf4j-api" % Versions.Slf4j,
  "org.slf4j" % "jcl-over-slf4j" % Versions.Slf4j,
  "org.scalatest" %% "scalatest" % Versions.ScalaTest % "test,it",
  "org.scalacheck" %% "scalacheck" % Versions.ScalaCheck % "test,it"
) map {
/*
lazy val dependencies = Seq(
  //  "com.amazonaws" % "aws-java-sdk" % Versions.AwsSdk,
  "com.amazonaws" % "amazon-sqs-java-messaging-lib" % Versions.AwsSqsJms,
  "org.apache.avro" % "avro" % Versions.Avro,
  "org.apache.kafka" % "kafka-clients" % Versions.Kafka,
  "org.apache.kafka" % "connect-api" % Versions.Kafka,
  "io.confluent" % "kafka-avro-serializer" % Versions.Confluent,
  "io.confluent" % "kafka-schema-registry-client" % Versions.Confluent,
  "io.confluent" % "kafka-connect-avro-converter" % Versions.Confluent,
  "org.slf4j" % "slf4j-api" % Versions.Slf4j,
  "org.slf4j" % "jcl-over-slf4j" % Versions.Slf4j,
  "org.scalatest" %% "scalatest" % Versions.ScalaTest % "test,it",
  "org.scalacheck" %% "scalacheck" % Versions.ScalaCheck % "test,it"
) map {
*/
  _.excludeAll(
    ExclusionRule(name = "javax.activation"),
    ExclusionRule(name = "javax.mail.glassfish"),
    ExclusionRule(name = "javax.transaction"),
    ExclusionRule(name = "org.apache.geronimo.specs"),
    ExclusionRule(name = "servlet-api"),
    ExclusionRule(name = "jsr305"),
    ExclusionRule(name = "commons-logging"),
    ExclusionRule(organization = "commons-beanutils"),
    ExclusionRule(organization = "org.ow2.asm", name = "asm"),
    ExclusionRule(organization = "org.jboss.netty"),
    ExclusionRule(organization = "org.apache.spark.unused"),
    ExclusionRule(organization = "com.esotericsoftware")
  )
}

libraryDependencies ++= dependencies

val dpIntegrationTest: Configuration = config("it") extend Test
lazy val root = project.in(file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys ++= Seq[BuildInfoKey](
      "latestGitTagVersion" -> "git describe --abbrev=0 --tags".!!.trim
    )
  )
  .configs(dpIntegrationTest)
  .settings(Defaults.itSettings: _*)

// This allows the fat jar to be published to artifactory as part of the release process.
artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.copy(`classifier` = Some("assembly"))
}
addArtifact(artifact in (Compile, assembly), assembly)

// Must for for javaOptions to be passed properly from outside sbt.
fork := true

// Standard assembly
assemblyJarName in assembly := s"${name.value}.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = true)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "*") => MergeStrategy.discard
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
  case PathList("org", "apache", "kafka", xs@_*) => MergeStrategy.discard
  case PathList("org", "apache", "zookeeper", xs@_*) => MergeStrategy.discard
  case PathList("io", "confluent", xs@_*) => MergeStrategy.discard
  case PathList("org", "apache", xs@_*) => MergeStrategy.last
  case PathList("com", "datastax", "driver", "core", "Driver.properties") => MergeStrategy.first
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
  case PathList("com", "google", "common", xs@_*) => MergeStrategy.last
  case PathList("org.apache.geronimo.specs", xs@_*) => MergeStrategy.discard
  case PathList("org", "apache", "geronimo", "specs", xs@_*) => MergeStrategy.discard
  case PathList("javax", "xml", xs@_*) => MergeStrategy.last
  case PathList("org", "joda", "time", xs@_*) => MergeStrategy.first
  case PathList("plugin.properties") => MergeStrategy.discard

  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

test in assembly := {}
test in(dpIntegrationTest, assembly) := {}
