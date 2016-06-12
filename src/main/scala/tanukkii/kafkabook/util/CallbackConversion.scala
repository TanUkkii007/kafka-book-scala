package tanukkii.kafkabook.util

import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.clients.producer.{RecordMetadata, Callback}
import org.apache.kafka.common.TopicPartition

trait CallbackConversion {

  implicit def functionToCallback(f: (RecordMetadata, Exception) => Unit): Callback = new Callback {
    def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = f(metadata, exception)
  }

  implicit def functionToOffsetCommitCallback(f: (java.util.Map[TopicPartition, OffsetAndMetadata], Exception) => Unit): OffsetCommitCallback = new OffsetCommitCallback {
    def onComplete(offsets: java.util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = f(offsets, exception)
  }
}
