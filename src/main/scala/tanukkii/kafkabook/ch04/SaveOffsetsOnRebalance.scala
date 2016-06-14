package tanukkii.kafkabook.ch04

import java.util

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer, ConsumerRebalanceListener}
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap

object VirtualDB {
  private val internal: TrieMap[String, Any] = TrieMap()
  def put(key: String, value: Any): Unit = internal += (key -> value)
  def get(key: String): Option[Any] = internal.get(key)
  def commitTransaction(): Unit = ()
}

class SaveOffsetsOnRebalance[K, V](consumer: KafkaConsumer[K, V]) extends ConsumerRebalanceListener{
  def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = VirtualDB.commitTransaction()

  def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
    partitions.asScala.foreach { partition =>
      consumer.seek(partition, VirtualDB.get(s"${partition.topic()}-${partition.partition()}").get.asInstanceOf[Long])
    }
  }

}

object SaveOffsetsOnRebalanceExample extends App {
  val props: Map[String, AnyRef] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "group.id" -> "CountryCounter",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "enable.auto.commit" -> false.asInstanceOf[AnyRef]
  )
  val consumer = new KafkaConsumer[String, String](props.asJava)

  consumer.subscribe(java.util.Collections.singletonList("CustomerCountry"), new SaveOffsetsOnRebalance(consumer))
  consumer.poll(0)

  consumer.assignment().asScala.foreach { partition =>
    consumer.seek(partition, VirtualDB.get(s"${partition.topic()}-${partition.partition()}").getOrElse(0L).asInstanceOf[Long])

    while (true) {
      val records = consumer.poll(100)
      records.asScala.zipWithIndex.foreach { v =>
        val (record, index) = v
        processRecord(record)
        VirtualDB.put(s"record-$index", record)
        VirtualDB.put(s"${record.topic()}-${record.partition()}", record.offset())
      }
      VirtualDB.commitTransaction()
    }
  }

  consumer.close()

  def processRecord[K, V](record: ConsumerRecord[K, V]): Unit = println(record)
}