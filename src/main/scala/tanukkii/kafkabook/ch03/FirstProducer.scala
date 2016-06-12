package tanukkii.kafkabook.ch03

import _root_.tanukkii.kafkabook.util.CallbackConversion
import org.apache.kafka.clients.producer.{RecordMetadata, ProducerRecord, KafkaProducer}
import scala.collection.JavaConverters._

object FirstProducer extends App with CallbackConversion {

  val kafkaProps = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
  )

  val producer = new KafkaProducer[String, String](kafkaProps.asInstanceOf[Map[String, AnyRef]].asJava)

  val record = new ProducerRecord("CustomerCountry", "Precision Products", "France")

  try {
    producer.send(record)
  } catch {
    case e: Exception => e.printStackTrace()
  }

  val result = producer.send(record).get()
  println(result)

  val record2 = new ProducerRecord("CustomerCountry", "Biomedical Materials", "USA")
  producer.send(record2, (metadata: RecordMetadata, exception: Exception) => {
    if (exception != null) exception.printStackTrace()
    else println(metadata)
  })

  Thread.sleep(1000)
}
