package tanukkii.kafkabook.ch03

import java.util

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.record.InvalidRecordException

import scala.util.hashing.MurmurHash3

class BananaPartitioner extends Partitioner{
  def close(): Unit = {}

  def partition(topic: String, key: scala.Any, keyBytes: Array[Byte], value: scala.Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    val partitions = cluster.partitionsForTopic(topic)
    val numPartitions = partitions.size()
    Option(keyBytes) match {
      case None => throw new InvalidRecordException("We expect all messages to have customer name as key")
      case Some(bytes) if bytes.toString == "Banana" => numPartitions
      case Some(bytes) => Math.abs(MurmurHash3.bytesHash(bytes)) % (numPartitions - 1)
    }
  }

  def configure(configs: util.Map[String, _]): Unit = {}
}
