import sbt._
import Keys._

object ScnBuild extends Build {
  var commonResolvers = Seq(
    // local snapshot support
    ScalaToolsSnapshots,

    "Maven.org" at "http://repo1.maven.org/maven2",
    "Sun Maven2 Repo" at "http://download.java.net/maven/2",
    "Scala-Tools" at "http://scala-tools.org/repo-releases/",
    "Sun GF Maven2 Repo" at "http://download.java.net/maven/glassfish",
    "Oracle Maven2 Repo" at "http://download.oracle.com/maven",
    "Sonatype" at "http://oss.sonatype.org/content/repositories/release"
  )

  var commonDeps = Seq(
    "com.wajam" %% "nrv-core" % "0.1-SNAPSHOT",
    "com.wajam" %% "nrv-zookeeper" % "0.1-SNAPSHOT",
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
    version := "0.1-SNAPSHOT"
  )

  lazy val root = Project(
    id = "scn",
    base = file("."),
    settings = defaultSettings ++ Seq(
      // some other
    )
  ) configs (IntegrationTest) aggregate (core)

  lazy val core = Project(
    id = "scn-core",
    base = file("scn-core"),
    settings = defaultSettings ++ Seq(
      // some other
    )
  ) configs (IntegrationTest)
}

