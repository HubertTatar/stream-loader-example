package io.huta.sle

import io.huta.sle.dataproducer.{AdminConnectionProps, Logging, ProducerDefault, SetupTopic}
import io.huta.sle.proto.ComplexTypeOuterClass.ComplexType
import io.huta.sle.proto.HeaderOuterClass.Header
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{BytesSerializer, StringSerializer}
import org.apache.kafka.common.utils.Bytes

import java.time.{LocalDateTime, ZoneId}
import java.util.{Properties, UUID}
import scala.util.Random

/*
  First run class with setup method to create topic in Kafka cluster
  Then run producer to run data
 */
object DataProducerRunner extends AdminConnectionProps with ProducerDefault with SetupTopic with Logging {

  def main(args: Array[String]): Unit = {
//    setup(kfkProps())
    producer(producerProperties()).run()
  }

  private def setup(props: Properties) = {
    setupTopics(props, List("complex_topic"))
  }

  private def producer(props: Properties): Runnable =
    () => {
      val producer = new KafkaProducer(props, new StringSerializer, new BytesSerializer)
      val random = Random
      for (i <- 0 to 1_000_01) {
        val newRecord = createNewRecord(random)
        val producerRecord =
          new ProducerRecord[String, Bytes]("complex_topic", s"$i", Bytes.wrap(newRecord.toByteArray))
        producer.send(producerRecord)
        // produce duplicates
        if (i % 10 == 0) {
          producer.send(producerRecord)
        }
      }

      producer.flush()
      producer.close()
      log.info("producer stopped")
    }

  private def createNewRecord(random: Random): ComplexType = {
    val now = LocalDateTime.now()
    val nowAtUtc = now.atZone(ZoneId.of("UTC"))

    val header = Header
      .newBuilder()
      .setUuid(UUID.randomUUID().toString)
      .setCreationTime(nowAtUtc.toInstant.toEpochMilli)
      .setRecentProducer("recent_one")
      .setOriginalProducer("original_one")
      .build()

    ComplexType
      .newBuilder()
      .setHeader(header)
      .setId(0)
      .setName(random.alphanumeric.take(10).mkString)
      .setPrice(random.between(0.0, 100.0))
      .setIsOn(random.nextBoolean())
      .addSomethings(random.nextInt())
      .addSomethings(random.nextInt())
      .build()
  }

}
