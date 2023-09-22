package io.huta.sle.loader

import com.adform.streamloader.sink.Sink
import com.typesafe.config.Config
import io.micrometer.core.instrument.MeterRegistry

trait Loader {
  def buildSink(): Sink

  def close(): Unit
}
