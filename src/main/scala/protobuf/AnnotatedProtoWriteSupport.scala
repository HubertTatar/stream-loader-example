package io.huta.sle
package protobuf

import com.google.protobuf.{Descriptors, Message}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.apache.parquet.proto.ProtoWriteSupport
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{LogicalTypeAnnotation, Types}

import scala.reflect.ClassTag

case class KafkaMetadata(topic: String, partition: Int, offset: Long, timestamp: Long, watermark: Long)
case class AnnotatedProtoRecord[R <: Message](record: R, metadata: KafkaMetadata)

/** Support for writing parsed protobuf messages to parquet together with additional metadata. The implementation makes
  * used of the original [[org.apache.parquet.proto.ProtoWriteSupport]] class, but overrides the schema generation and
  * record writing to inject the additional fields.
  *
  * @tparam R
  *   Type of the protobuf message to write.
  */
class AnnotatedProtoWriteSupport[R <: Message: ClassTag] extends WriteSupport[AnnotatedProtoRecord[R]] {

  private val recordClass = implicitly[ClassTag[R]].runtimeClass.asInstanceOf[Class[R]]
  private val recordTopLevelFieldCount =
    recordClass.getMethod("getDescriptor").invoke(recordClass).asInstanceOf[Descriptors.Descriptor].getFields.size()

  private val KAFKA_METADATA_MESSAGE_NAME = "_kafka"
  private val KAFKA_TOPIC_FIELD_NAME = "topic"
  private val KAFKA_PARTITION_FIELD_NAME = "partition"
  private val KAFKA_OFFSET_FIELD_NAME = "offset"
  private val KAFKA_TIMESTAMP_FIELD_NAME = "timestamp"
  private val KAFKA_WATERMARK_FIELD_NAME = "watermark"

  /** An extended ProtoWriteSupport class that appends extra metadata fields at the end of each record. It cannot be
    * used directly, as `ProtoWriteSupport` requires records to be protobuf Messages, so there's no way to pass extra
    * metadata in `write`, so we provide an overloaded `write` method.
    */
  private class InnerProtoWriteSupport extends ProtoWriteSupport[R](recordClass) {
    override def init(configuration: Configuration): WriteSupport.WriteContext = {
      val kafkaMetadataSchema = Types
        .buildMessage()
        .group(Repetition.REQUIRED)
        .required(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named(KAFKA_TOPIC_FIELD_NAME)
        .required(PrimitiveTypeName.INT32)
        .named(KAFKA_PARTITION_FIELD_NAME)
        .required(PrimitiveTypeName.INT64)
        .named(KAFKA_OFFSET_FIELD_NAME)
        .required(PrimitiveTypeName.INT64)
        .named(KAFKA_TIMESTAMP_FIELD_NAME)
        .required(PrimitiveTypeName.INT64)
        .named(KAFKA_WATERMARK_FIELD_NAME)
        .named(KAFKA_METADATA_MESSAGE_NAME)
        .named("kafka_metadata_group") // name is irrelevant, as it will disappear after union

      // Add the extra fields to the end of the schema as the writer expects to find proto fields at the beginning
      val context = super.init(configuration)
      val schema = context.getSchema.union(kafkaMetadataSchema)
      new WriteContext(schema, context.getExtraMetaData)
    }

    private var recordConsumer: RecordConsumer = _

    override def prepareForWrite(consumer: RecordConsumer): Unit = {
      // Give the base class a modified record consumer that does NOT end messages,
      // as we want to add the extra metadata fields at the end.
      recordConsumer = consumer
      val wrappedRecordConsumer = new WrappingRecordConsumer(recordConsumer) {
        override def endMessage(): Unit = {}
      }
      super.prepareForWrite(wrappedRecordConsumer)
    }

    override def write(record: R): Unit = {
      throw new UnsupportedOperationException("This class cannot be used directly")
    }

    def write(parsedRecord: R, metadata: KafkaMetadata): Unit = {
      super.write(parsedRecord)

      recordConsumer.startField(KAFKA_METADATA_MESSAGE_NAME, recordTopLevelFieldCount)
      recordConsumer.startGroup()
      writePrimitiveStringField(KAFKA_TOPIC_FIELD_NAME, 0, metadata.topic)
      writePrimitiveIntField(KAFKA_PARTITION_FIELD_NAME, 1, metadata.partition)
      writePrimitiveLongField(KAFKA_OFFSET_FIELD_NAME, 2, metadata.offset)
      writePrimitiveLongField(KAFKA_TIMESTAMP_FIELD_NAME, 3, metadata.timestamp)
      writePrimitiveLongField(KAFKA_WATERMARK_FIELD_NAME, 4, metadata.watermark)
      recordConsumer.endGroup()
      recordConsumer.endField(KAFKA_METADATA_MESSAGE_NAME, recordTopLevelFieldCount)

      // Finally end the message that the base class started.
      recordConsumer.endMessage()
    }

    private def writePrimitiveLongField(field: String, idx: Int, value: Long): Unit = {
      recordConsumer.startField(field, idx)
      recordConsumer.addLong(value)
      recordConsumer.endField(field, idx)
    }

    private def writePrimitiveIntField(field: String, idx: Int, value: Int): Unit = {
      recordConsumer.startField(field, idx)
      recordConsumer.addInteger(value)
      recordConsumer.endField(field, idx)
    }

    private def writePrimitiveStringField(field: String, idx: Int, value: String): Unit = {
      recordConsumer.startField(field, idx)
      recordConsumer.addBinary(Binary.fromString(value))
      recordConsumer.endField(field, idx)
    }
  }

  private val protoWriteSupport = new InnerProtoWriteSupport

  override def init(configuration: Configuration): WriteContext = {
    protoWriteSupport.init(configuration)
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    protoWriteSupport.prepareForWrite(recordConsumer)
  }

  override def write(r: AnnotatedProtoRecord[R]): Unit = {
    protoWriteSupport.write(r.record, r.metadata)
  }
}
