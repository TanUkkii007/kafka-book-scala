package tanukkii.kafkabook.ch03

import customerManagement.avro.Customer
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import scala.collection.JavaConverters._

object AvroProducer extends App {

  val props: Map[String, AnyRef] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> "io.confluent.kafka.serializers.KafkaAvroSerializer",
    "value.serializer" -> "io.confluent.kafka.serializers.KafkaAvroSerializer",
    "schema.registry.url" -> "http://localhost:8081"
  )

  val topic = "customerContacts"
  val waitInterval = 500

  val producer = new KafkaProducer[Int, Customer](props.asJava)

  Stream.from(0).take(10).foreach { id =>
    val customer = new Customer(id, "John Doe", "john@example.com")
    println("Generated customer " + customer.toString())
    val record = new ProducerRecord(topic, customer.getId().toInt, customer)
    producer.send(record)
  }

}
