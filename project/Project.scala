import sbt._
import Keys._
import com.typesafe.sbt.SbtStartScript

object ScnBuild extends Build {

  var commonResolvers = Seq(
    // local snapshot support
    ScalaToolsSnapshots,

    "Maven.org" at "http://repo1.maven.org/maven2",
    "Sun Maven2 Repo" at "http://download.java.net/maven/2",
    "Scala-Tools" at "http://scala-tools.org/repo-releases/",
    "Sun GF Maven2 Repo" at "http://download.java.net/maven/glassfish",
    "Oracle Maven2 Repo" at "http://download.oracle.com/maven",
    "Sonatype" at "http://oss.sonatype.org/content/repositories/release",
    "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    "Scallop" at "http://mvnrepository.com/",
    "Twitter" at "http://maven.twttr.com/",
    "Wajam" at "http://ci1.cx.wajam/"
  )

  var commonDeps = Seq(
    "org.slf4j" % "slf4j-log4j12" % "1.6.4",
    "commons-configuration" % "commons-configuration" % "1.6",
    "log4j" % "log4j" % "1.2.15" exclude("javax.jms", "jms") exclude("com.sun.jmx", "jmxri") exclude("com.sun.jdmk", "jmxtools"),
    "com.wajam" %% "nrv-core" % "0.1-SNAPSHOT" exclude("org.slf4j", "slf4j-nop"),
    "com.wajam" %% "nrv-core" % "0.1-SNAPSHOT",
    "com.wajam" %% "nrv-extension" % "0.1-SNAPSHOT",
    "com.wajam" %% "nrv-zookeeper" % "0.1-SNAPSHOT",
    "com.wajam" %% "nrv-scribe" % "0.1-SNAPSHOT",
    "com.yammer.metrics" %% "metrics-scala" % "2.1.2",
    "com.yammer.metrics" % "metrics-graphite" % "2.1.2",
    "org.rogach" %% "scallop" % "0.6.0",
    "org.scalatest" %% "scalatest" % "1.7.1" % "test,it",
    "junit" % "junit" % "4.10" % "test,it",
    "org.mockito" % "mockito-core" % "1.9.0" % "test,it"
  )

  val defaultSettings = Defaults.defaultSettings ++ Defaults.itSettings ++ Seq(
    libraryDependencies ++= commonDeps,
    resolvers ++= commonResolvers,
    retrieveManaged := true,
    publishMavenStyle := true,
    organization := "com.wajam",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.9.1"
  )

  lazy val root = Project("scn", file("."))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .settings(SbtStartScript.startScriptForClassesSettings: _*)
    .aggregate(core)

  lazy val core = Project("scn-core", file("scn-core"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .settings(SbtStartScript.startScriptForClassesSettings: _*)
    .settings(mainClass in (Compile) := Some("com.wajam.scn.ScnServer"))

  import sbtprotobuf.{ProtobufPlugin => PB}

  val protobufSettings = PB.protobufSettings ++ Seq(
    javaSource in PB.protobufConfig <<= (sourceDirectory in Compile)(_ / "java")
  )

  // We keep it as a separate projet, to avoid a dependency on protoc
  // The protobuf file are under version control, so no need to generate them everytime.
  // To generate them run sbt shell them run ";project scn-protobuf ;protobuf:generate"
  lazy val protobuf = Project("scn-protobuf", file("scn-core"))
    .settings(protobufSettings: _*)
}

