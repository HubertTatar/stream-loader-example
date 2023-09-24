package io.huta.sle.dataproducer

import org.log4s.{Logger, getLogger}

trait Logging {
  val log: Logger = getLogger(getClass)
}
