import sbt._
import Keys._
import com.typesafe.startscript.StartScriptPlugin

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
    "Wajam" at "http://ci1.is.wajam/"
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
    .settings(StartScriptPlugin.startScriptForClassesSettings: _*)
    .aggregate(core)

  lazy val core = Project("scn-core", file("scn-core"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .settings(StartScriptPlugin.startScriptForClassesSettings: _*)
    .settings(mainClass in (Compile) := Some("com.wajam.scn.ScnServer"))
}

