package io.huta.sle.config

import com.adform.streamloader.util.Logging
import com.typesafe.config.Config

import scala.jdk.CollectionConverters._

object ConfigUtil extends Logging {
  def printConfig(cfg: Config): Unit = {
    val cfgString =
      cfg
        .entrySet()
        .asScala
        .map(c => s"\t${c.getKey} = ${c.getValue.unwrapped().toString}")
        .toList
        .sorted
        .mkString("\n")
    log.info(s"Application configuration values:\n$cfgString")
  }
}
