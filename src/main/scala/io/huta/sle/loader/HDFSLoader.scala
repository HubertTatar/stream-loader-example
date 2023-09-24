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
import com.adform.streamloader.util.{GaussianDistribution, TimeExtractor}
import com.google.protobuf.Message
import com.typesafe.config.Config
import io.huta.sle.config.ConfigExtensions.RichConfig
import io.huta.sle.deduplication.DeduplicatingRecordBatchingSink
import io.huta.sle.extension.{AnnotatedProtoParquetFileBuilder, AnnotatedProtoRecord, GenericRecordFormatter}
import org.apache.hadoop.fs.FileSystem

import java.time.{Duration, LocalDateTime, ZoneId}
import scala.reflect.ClassTag

abstract class HDFSLoader[R <: Message: ClassTag](
    fileSystem: FileSystem,
    config: Config
) extends Loader {

  def buildSink(): Sink = DeduplicatingRecordBatchingSink
    .builder()
    .recordBatcher(recordBatcher())
    .batchStorage(batchStorage(fileSystem))
    .batchCommitQueueSize(config.getInt("file.commit.queue.size"))
    .interval(StreamInterval.OffsetRange(config.getInt("key-cache-size")))
    .keyCacheSize(config.getInt("key-cache-size"))
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
          compression = parseCompression(config.getStringOpt("file.compression")),
          blockSize = config.getInt("file.block.size.bytes"),
          pageSize = config.getInt("file.page.size.bytes")
        )
      )
      .fileCommitStrategy(
        MultiFileCommitStrategy.anyFile(parseFixedFileCommitStrategy(config.getConfig("file.max")))
      )
      .build()
  }

  private def batchStorage(hadoopFileSystem: FileSystem): HadoopFileStorage[LocalDateTime] = {
    implicit val localDateTime: TimeExtractor[LocalDateTime] =
      (value: LocalDateTime) => Timestamp(value.atZone(ZoneId.of("UTC")).toInstant.toEpochMilli)

    HadoopFileStorage
      .builder()
      .hadoopFS(hadoopFileSystem)
      .stagingBasePath(config.getString("staging-directory"))
      .destinationBasePath(config.getString("base-directory"))
      .destinationFilePathFormatter(
        new TimePartitioningFilePathFormatter[LocalDateTime](
          Some(config.getString("file.time-partition.pattern")),
          None
        )
      )
      .build()
  }

  private def parseCompression(str: Option[String]): Compression =
    str
      .map(compressionStr =>
        Compression.tryParse(compressionStr) match {
          case Some(c) => c
          case None => throw new IllegalArgumentException(s"Unknown compression type '$compressionStr'")
        }
      )
      .getOrElse(Compression.NONE)

  private def parseFuzzyFileCommitStrategy(cfg: Config): FileCommitStrategy.FuzzyReachedAnyOf =
    FileCommitStrategy.FuzzyReachedAnyOf(
      fileOpenDurationDistribution = cfg
        .getConfigOpt("open-duration")
        .map(c => GaussianDistribution[Duration](c.getDuration("mean"), c.getDuration("stddev"))),
      fileSizeDistribution =
        cfg.getConfigOpt("size").map(c => GaussianDistribution[Long](c.getBytes("mean"), c.getBytes("stddev"))),
      recordsWrittenDistribution =
        cfg.getConfigOpt("records").map(c => GaussianDistribution[Long](c.getLong("mean"), c.getLong("stddev")))
    )(randomSeed = None)

  private def parseFixedFileCommitStrategy(cfg: Config): FileCommitStrategy.ReachedAnyOf =
    FileCommitStrategy.ReachedAnyOf(
      recordsWritten = cfg.getLongOpt("records"),
      fileOpenDuration = cfg.getDurationOpt("open-duration"),
      fileSize = cfg.getBytesOpt("size")
    )
}
