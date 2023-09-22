package io.huta.sle.config

import com.typesafe.config.Config

import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters._

object ConfigExtensions {
  implicit class RichConfig(cfg: Config) {
    def toProperties: Properties = {
      val props = new Properties()
      cfg.entrySet().asScala.foreach(e => props.put(e.getKey, e.getValue.unwrapped()))
      props
    }

    def getConfigOpt(path: String): Option[Config] = if (cfg.hasPath(path)) Some(cfg.getConfig(path)) else None

    def getStringOpt(path: String): Option[String] = if (cfg.hasPath(path)) Some(cfg.getString(path)) else None

    def getIntOpt(path: String): Option[Int] = if (cfg.hasPath(path)) Some(cfg.getInt(path)) else None

    def getLongOpt(path: String): Option[Long] = if (cfg.hasPath(path)) Some(cfg.getLong(path)) else None

    def getBytesOpt(path: String): Option[Long] = if (cfg.hasPath(path)) Some(cfg.getBytes(path)) else None

    def getDurationOpt(path: String): Option[Duration] = if (cfg.hasPath(path)) Some(cfg.getDuration(path)) else None
  }
}
