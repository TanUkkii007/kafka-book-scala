package tanukkii.kafkabook.example.stream

import org.apache.kafka.streams.kstream.KeyValueMapper


trait KeyValueMapperConversion {
  implicit def functionToKeyValueMapper[K, V, R](f: (K, V) => R): KeyValueMapper[K, V, R] = new KeyValueMapper[K, V, R] {
    def apply(key: K, value: V): R = f(key, value)
  }
}
