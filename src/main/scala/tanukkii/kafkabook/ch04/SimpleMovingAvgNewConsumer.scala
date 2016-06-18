package tanukkii.kafkabook.ch04

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import scala.collection.JavaConverters._

object SimpleMovingAvgNewConsumer extends App {

  val props: Map[String, AnyRef] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "group.id" -> "SimpleMovingAvg",
    "auto.offset.reset" -> "earliest",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
  )

  val consumer = new KafkaConsumer[String, String](props.asJava)

  val mainThread = Thread.currentThread()

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run() = {
      println("Starting exit...")
      consumer.wakeup()
      mainThread.join()
    }
  })

  try {
    consumer.subscribe(java.util.Collections.singletonList("CustomerCountry"))
    while (true) {
      val records = consumer.poll(100)
      println(System.currentTimeMillis() + "  --  waiting for data...")
      records.asScala.foreach { record =>
        printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value())

      }

      consumer.assignment().asScala.foreach { tp =>
        println("Committing offset at position:" + consumer.position(tp))
        consumer.commitSync()
      }
    }
  } catch {
    case e: WakeupException => e.printStackTrace()
  } finally {
    consumer.close()
  }
}
