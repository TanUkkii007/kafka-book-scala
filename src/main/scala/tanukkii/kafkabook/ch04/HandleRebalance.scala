package tanukkii.kafkabook.ch04

import java.util

import org.apache.kafka.clients.consumer.{OffsetAndMetadata, KafkaConsumer, ConsumerRebalanceListener}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import tanukkii.kafkabook.util.CallbackConversion
import scala.collection.JavaConverters._

class HandleRebalance[K, V](consumer: KafkaConsumer[K, V]) extends ConsumerRebalanceListener{
  def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {}

  def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = consumer.commitSync()
}

object HandleRebalanceExample extends App with CallbackConversion {
  val props: Map[String, AnyRef] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "group.id" -> "CountryCounter",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "enable.auto.commit" -> false.asInstanceOf[AnyRef]
  )
  val consumer = new KafkaConsumer[String, String](props.asJava)

  var currentOffsets: Map[TopicPartition, OffsetAndMetadata] = Map()

  try {
    consumer.subscribe(java.util.Collections.singletonList("CustomerCountry"), new HandleRebalance(consumer))

    while (true) {
      val records = consumer.poll(100)
      records.asScala.foreach { record =>
        printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
        record.topic(), record.partition(), record.offset(), record.key(), record.value())
        currentOffsets = currentOffsets.updated(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()))
      }
      consumer.commitAsync(currentOffsets.asJava, (offsets: java.util.Map[TopicPartition, OffsetAndMetadata], exception: Exception) => {

      })
    }
  } catch {
    case e: WakeupException =>
    case e: Exception => e.printStackTrace()
  } finally {
    try {
      consumer.commitSync(currentOffsets.asJava)
    } finally {
      consumer.close()
    }
  }

}