name := "otus_kafka_producer"

version := "0.1"

scalaVersion := "2.13.4"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.6.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)

libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.1"

