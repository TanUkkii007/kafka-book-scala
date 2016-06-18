package tanukkii.kafkabook.ch04


import _root_.customerManagement.avro.Customer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import scala.collection.JavaConverters._

object AvroConsumer extends App {

  val props: Map[String, AnyRef] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "group.id" -> "CountryCounter",
    "key.deserializer" -> "io.confluent.kafka.serializers.KafkaAvroDeserializer",
    "value.deserializer" -> "io.confluent.kafka.serializers.KafkaAvroDeserializer",
    "schema.registry.url" -> "http://localhost:8081"
  )

  val schemaString =
    """
      |{
      |  "namespace": "customerManagement.avro",
      |  "type": "record",
      |  "name": "Customer",
      |  "fields": [
      |    {"name": "id", "type": "int"},
      |    {"name": "name",  "type": "string"},
      |    {"name": "email", "type": ["null", "string"], "default": "null"}
      |  ]
      |}
    """.stripMargin

  val parser = new Schema.Parser()
  val schema = parser.parse(schemaString)

  val topic = "customerContacts"

  val consumer = new KafkaConsumer[Int, GenericRecord](props.asJava)

  val mainThread = Thread.currentThread()

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run() = {
      consumer.wakeup()
      mainThread.join()
    }
  })

  consumer.subscribe(java.util.Collections.singletonList(topic))

  println("Reading topic:" + topic)

  try {
    while (true) {
      val records = consumer.poll(1000)
      records.asScala.foreach { record =>
        println("Current customer name is: " + record.value().get("name"))
      }
      consumer.commitSync()
    }
  } catch {
    case e: WakeupException => e.printStackTrace()
  } finally {
    consumer.close()
  }
}
