package tanukkii.kafkabook.ch04

import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.{TopicPartition, PartitionInfo}
import scala.collection.JavaConverters._

object StandAloneConsumer extends App {

  val props: Map[String, AnyRef] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
  )
  val consumer = new KafkaConsumer[String, String](props.asJava)

  try {
    val partitionInfos: util.List[PartitionInfo] = consumer.partitionsFor("CustomerCountry")

    if (partitionInfos != null) {
      val partitions = partitionInfos.asScala.foldLeft(List.empty[TopicPartition]) { (acc, partition) =>
        new TopicPartition(partition.topic(), partition.partition()) :: acc
      }
      consumer.assign(partitions.asJava)

      while (true) {
        val records = consumer.poll(1000)
        records.asScala.foreach { record =>
          println("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value())
        }
        consumer.commitSync()
      }
    }
  } finally {
    consumer.close()
  }

}
