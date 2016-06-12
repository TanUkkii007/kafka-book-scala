scalaVersion := "2.11.8"

val kafkaVersion = "0.10.0.0"

val avroVersion = "1.8.1"

val confluentVersion = "3.0.0"

resolvers += "confluent" at "http://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.avro" % "avro" % avroVersion,
  "org.apache.avro" % "avro-tools" % avroVersion % "test",
  "io.confluent" % "kafka-avro-serializer" % confluentVersion
)
