package tanukkii.kafkabook.example.stream

import org.apache.kafka.streams.kstream.ValueJoiner

trait ValueJoinerConversion {
  implicit def functionToValueJoiner[A, B, C](f: (A, B) => C): ValueJoiner[A, B, C] = new ValueJoiner[A, B, C] {
    def apply(value1: A, value2: B): C = f(value1, value2)
  }
}
