package io.huta.sle.config

import com.adform.streamloader.source.KafkaSource

import java.time.Duration
import java.util.Properties

object KafkaSources {
  def kafkaSource(properties: Properties, topics: Seq[String]): KafkaSource =
    KafkaSource
      .builder()
      .consumerProperties(properties)
      .pollTimeout(Duration.ofSeconds(1))
      .topics(topics)
      .build()
}
