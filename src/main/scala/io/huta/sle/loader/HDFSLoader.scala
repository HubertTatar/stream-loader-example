package io.huta.sle.loader

import com.adform.streamloader.hadoop.HadoopFileStorage
import com.adform.streamloader.model.{StreamInterval, Timestamp}
import com.adform.streamloader.sink.Sink
import com.adform.streamloader.sink.file.{
  Compression,
  FileCommitStrategy,
  MultiFileCommitStrategy,
  PartitioningFileRecordBatcher,
  TimePartitioningFilePathFormatter
}
import com.adform.streamloader.util.TimeExtractor
import com.google.protobuf.Message
import com.typesafe.config.Config
import io.huta.sle.deduplication.DeduplicatingRecordBatchingSink
import io.huta.sle.protobuf.{AnnotatedProtoParquetFileBuilder, AnnotatedProtoRecord, GenericRecordFormatter}
import io.micrometer.core.instrument.MeterRegistry
import org.apache.hadoop.fs.FileSystem

import java.time.{Duration, LocalDateTime, ZoneId}
import scala.reflect.ClassTag

abstract class HDFSLoader[R <: Message: ClassTag](fileSystem: FileSystem, config: Config, metricRegistry: MeterRegistry)
    extends Loader {

  def buildSink(): Sink = DeduplicatingRecordBatchingSink
    .builder()
    .recordBatcher(recordBatcher())
    .batchStorage(batchStorage(fileSystem))
    .batchCommitQueueSize(1)
    .interval(StreamInterval.OffsetRange(1000))
    .build()

  def close(): Unit = fileSystem.close()

  private def recordBatcher(): PartitioningFileRecordBatcher[LocalDateTime, AnnotatedProtoRecord[R]] = {
    PartitioningFileRecordBatcher
      .builder()
      .recordFormatter(new GenericRecordFormatter[R])
      .recordPartitioner((r, _) =>
        r.watermark.toDateTime
          .withMinute(0)
          .withSecond(0)
          .withNano(0)
      )
      .fileBuilderFactory(_ =>
        new AnnotatedProtoParquetFileBuilder[R](
          Compression.NONE,
          blockSize = 134217728,
          pageSize = 1048576
        )
      )
      .fileCommitStrategy(
        MultiFileCommitStrategy.anyFile(parseFixedFileCommitStrategy())
      )
      .build()
  }

  private def parseFixedFileCommitStrategy(): FileCommitStrategy.ReachedAnyOf =
    FileCommitStrategy.ReachedAnyOf(
      recordsWritten = Some(6),
      fileOpenDuration = Some(Duration.ofMinutes(5)),
      fileSize = Some(256000000)
    )

  private def batchStorage(hadoopFileSystem: FileSystem): HadoopFileStorage[LocalDateTime] = {
    implicit val localDateTime: TimeExtractor[LocalDateTime] =
      (value: LocalDateTime) => Timestamp(value.atZone(ZoneId.of("UTC")).toInstant.toEpochMilli)

    HadoopFileStorage
      .builder()
      .hadoopFS(hadoopFileSystem)
      .stagingBasePath("/data/stage")
      .destinationBasePath("/data/ingested")
      .destinationFilePathFormatter(
        new TimePartitioningFilePathFormatter[LocalDateTime](
          Some("'dt='yyyy'_'MM'_'dd"),
          None
        )
      )
      .build()
  }
}
