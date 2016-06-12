package tanukkii.kafkabook.ch04

import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._

object FirstConsumer extends App {

  val props: Map[String, AnyRef] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "group.id" -> "CountryCounter",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
  )
  val consumer = new KafkaConsumer[String, String](props.asJava)

  consumer.subscribe(java.util.Collections.singletonList("CustomerCountry"))

  var customerCountryMap = Map.empty[String, Int]

  try {
    while (true) {
      val records = consumer.poll(100)
      records.asScala.foreach { record =>
        printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
        record.topic(), record.partition(), record.offset(), record.key(), record.value())
        customerCountryMap = customerCountryMap.updated(record.value(), customerCountryMap.getOrElse(record.value(), 0) + 1)
        println(customerCountryMap)
      }
    }
  } finally {
    consumer.close()
  }
}
