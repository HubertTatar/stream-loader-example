package io.huta.sle.protobuf

import org.apache.parquet.io.api.{Binary, RecordConsumer}

/** A wrapper around a [[org.apache.parquet.io.api.RecordConsumer]] that allows one to modify it's behavior. By default
  * all methods just pass through.
  */
class WrappingRecordConsumer(consumer: RecordConsumer) extends RecordConsumer {
  override def startMessage(): Unit = consumer.startMessage()
  override def endMessage(): Unit = consumer.endMessage()
  override def startField(field: String, index: Int): Unit = consumer.startField(field, index)
  override def endField(field: String, index: Int): Unit = consumer.endField(field, index)
  override def startGroup(): Unit = consumer.startGroup()
  override def endGroup(): Unit = consumer.endGroup()
  override def addInteger(value: Int): Unit = consumer.addInteger(value)
  override def addLong(value: Long): Unit = consumer.addLong(value)
  override def addBoolean(value: Boolean): Unit = consumer.addBoolean(value)
  override def addBinary(value: Binary): Unit = consumer.addBinary(value)
  override def addFloat(value: Float): Unit = consumer.addFloat(value)
  override def addDouble(value: Double): Unit = consumer.addDouble(value)
}
