package tanukkii.kafkabook.ch03.tanukkii.kafkabook.util

import org.apache.kafka.clients.producer.{RecordMetadata, Callback}

trait CallbackConversion {

  implicit def functionToCallback(f: (RecordMetadata, Exception) => Unit): Callback = new Callback {
    def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = f(metadata, exception)
  }

}
