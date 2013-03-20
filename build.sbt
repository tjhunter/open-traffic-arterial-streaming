name := "arterial-modelA"

version := "0.1-SNAPSHOT"

scalaVersion := "2.9.1"

resolvers += "Local Maven Repository" at "file://"+Path.userHome+"/.m2/repository"

resolvers += "Codahale" at "http://repo.codahale.com"

resolvers ++= Seq(
            "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
            "PATH public snapshots" at "http://maven.path.berkeley.edu/artifactory/public/"
)

libraryDependencies  += "org.scalanlp" % "breeze-learn_2.9.2" % "0.2-SNAPSHOT"

libraryDependencies += "org.spark-project" % "spark-core_2.9.2" % "0.7.0-SNAPSHOT"

libraryDependencies += "org.spark-project" % "spark-streaming_2.9.2" % "0.7.0-SNAPSHOT"

// Dependencies to represent and manipulate time

libraryDependencies += "edu.berkeley.path" % "core" % "0.2-SNAPSHOT"

libraryDependencies += "edu.berkeley.path" % "netconfig" % "0.2-SNAPSHOT"

libraryDependencies += "edu.berkeley.path" % "netconfig-io" % "0.2-SNAPSHOT"

libraryDependencies += "edu.berkeley.path" % "arterial" % "0.2-SNAPSHOT"

libraryDependencies += "cc.mallet" % "mallet" % "2.0.7"

libraryDependencies += "com.github.scopt" %% "scopt" % "1.1.3"

libraryDependencies += "com.codahale" %% "jerkson" % "0.5.0"

libraryDependencies += "com.github.scala-incubator.io" %% "scala-io-core" % "0.4.1-seq"

libraryDependencies += "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.1-seq"

libraryDependencies += "org.scalatest" %% "scalatest" % "1.8" % "test"

libraryDependencies += "com.googlecode.efficient-java-matrix-library" % "ejml" % "0.20"

libraryDependencies += "org.scalaj" % "scalaj-time_2.9.2" % "0.6"