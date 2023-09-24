package io.huta.sle.dataproducer

import org.apache.kafka.clients.producer.ProducerConfig

import java.util.Properties
import scala.jdk.CollectionConverters._

trait ProducerDefault {
  def producerProperties(): Properties = {
    val props = new Properties()
    props.putAll(
      Map(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094",
        ProducerConfig.ACKS_CONFIG -> "all"
      ).asJava
    )
    props
  }
}
