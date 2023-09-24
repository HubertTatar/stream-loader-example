package io.huta.sle.dataproducer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serdes

import java.util.Properties
import scala.jdk.CollectionConverters._

trait AdminConnectionProps {

  def kfkProps(): Properties = {
    val props = new Properties
    props.putAll(
      Map(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> Serdes.String().getClass,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> Serdes.Bytes().getClass
      ).asJava
    )
    props
  }

}
