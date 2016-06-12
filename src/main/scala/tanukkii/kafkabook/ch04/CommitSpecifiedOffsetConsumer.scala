package tanukkii.kafkabook.ch04

import org.apache.kafka.clients.consumer.{OffsetAndMetadata, CommitFailedException, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import tanukkii.kafkabook.util.CallbackConversion
import scala.collection.JavaConverters._

object CommitSpecifiedOffsetConsumer extends App with CallbackConversion {

  val props: Map[String, AnyRef] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "group.id" -> "CountryCounter",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "enable.auto.commit" -> false.asInstanceOf[AnyRef]
  )
  val consumer = new KafkaConsumer[String, String](props.asJava)

  consumer.subscribe(java.util.Collections.singletonList("CustomerCountry"))

  var customerCountryMap = Map.empty[String, Int]

  var currentOffsets: Map[TopicPartition, OffsetAndMetadata] = Map()
  var count = 0

  try {
    while (true) {
      val records = consumer.poll(100)
      records.asScala.foreach { record =>
        printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
          record.topic(), record.partition(), record.offset(), record.key(), record.value())
        customerCountryMap = customerCountryMap.updated(record.value(), customerCountryMap.getOrElse(record.value(), 0) + 1)
        println(customerCountryMap)

        currentOffsets = currentOffsets.updated(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()))
        if (count % 3 == 0) {
          consumer.commitAsync(currentOffsets.asJava, (offsets: java.util.Map[TopicPartition, OffsetAndMetadata], exception: Exception) => {
            if (exception != null) exception.printStackTrace()
            println(offsets.asScala)
          })
        }
        count += 1
      }
    }
  } finally {
    consumer.close()
  }
}
