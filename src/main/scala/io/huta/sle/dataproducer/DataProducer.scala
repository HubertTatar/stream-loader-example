package io.huta.sle.dataproducer

import org.apache.kafka.clients.producer.KafkaProducer

import java.util.Properties
import scala.util.Using

class DataProducer(properties: Properties) {

  def withProducer[K, E](fn: KafkaProducer[String, K] => E): Either[DataProducerError, E] = {
    try {
      Using.resource(new KafkaProducer[String, K](properties)) { producer =>
        Right(fn(producer))
      }
    } catch {
      case ex: Exception => Left(UnexpectedException(ex))
      case t: Throwable => throw t
    }
  }
}

sealed trait DataProducerError

case class UnexpectedException(exception: Exception) extends DataProducerError
