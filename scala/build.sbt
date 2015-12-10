resolvers += 
  "Cloudera Repositories" at "https://repository.cloudera.com/artifactory/cloudera-repos"

scalaVersion := "2.11.4"

libraryDependencies ++= Seq (
    "org.kududb" % "kudu-client" % "0.6.0" 
  )
