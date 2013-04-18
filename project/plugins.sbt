resolvers += Classpaths.typesafeResolver

resolvers += "gseitz@github" at "http://gseitz.github.com/maven/"

addSbtPlugin("com.typesafe.sbt" % "sbt-start-script" % "0.7.0")

addSbtPlugin("com.github.gseitz" % "sbt-protobuf" % "0.2.2")
