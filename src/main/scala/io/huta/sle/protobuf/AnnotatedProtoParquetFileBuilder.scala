package io.huta.sle.protobuf

import java.io.File
import com.adform.streamloader.sink.file._
import com.google.protobuf.Message
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import java.util.UUID
import scala.reflect.ClassTag

class AnnotatedProtoParquetFileBuilder[R <: Message: ClassTag](
    compression: Compression,
    blockSize: Long,
    pageSize: Int
)(implicit
    currentTimeMills: () => Long = () => System.currentTimeMillis()
) extends BaseFileBuilder[AnnotatedProtoRecord[R]]() {

  private class AnnotatedProtoParquetWriterBuilder(path: Path)
      extends ParquetWriter.Builder[AnnotatedProtoRecord[R], AnnotatedProtoParquetWriterBuilder](path) {
    override def self(): AnnotatedProtoParquetWriterBuilder = this

    override def getWriteSupport(conf: Configuration): WriteSupport[AnnotatedProtoRecord[R]] =
      new AnnotatedProtoWriteSupport[R]
  }

  private val parquetWriter = {
    val compressionCodecName: CompressionCodecName = compression match {
      case Compression.NONE => CompressionCodecName.UNCOMPRESSED
      case Compression.ZSTD => CompressionCodecName.ZSTD
      case Compression.GZIP => CompressionCodecName.GZIP
      case Compression.SNAPPY => CompressionCodecName.SNAPPY
      case Compression.LZ4 => CompressionCodecName.LZ4
      case _ => throw new UnsupportedOperationException(s"Compression '$compression' is unsupported in parquet")
    }

    val builder = new AnnotatedProtoParquetWriterBuilder(new Path(file.getAbsolutePath))
    builder
      .withCompressionCodec(compressionCodecName)
      .withRowGroupSize(blockSize)
      .withPageSize(pageSize)
      .withDictionaryPageSize(pageSize)
      .withDictionaryEncoding(true)
      .withValidation(true)
      .build()
  }

  override def write(record: AnnotatedProtoRecord[R]): Unit = {
    parquetWriter.write(record)
    super.write(record)
  }

  override protected def createFile(): File = {
    val tmpDir = System.getProperty("java.io.tmpdir")
    val filename = UUID.randomUUID().toString
    new File(s"$tmpDir/$filename.parquet")
  }

  override def getDataSize: Long = parquetWriter.getDataSize

  override def build(): Option[File] = {
    if (!isClosed) parquetWriter.close()
    super.build()
  }

  override def discard(): Unit = {
    if (!isClosed) parquetWriter.close()
    super.discard()
  }

}
