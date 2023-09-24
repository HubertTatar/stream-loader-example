package io.huta.sle.config

import com.adform.streamloader.hadoop.HadoopFileStorage
import com.adform.streamloader.model.{StreamInterval, Timestamp}
import com.adform.streamloader.sink.batch.RecordBatchingSink
import com.adform.streamloader.sink.file.{
  Compression,
  FileCommitStrategy,
  MultiFileCommitStrategy,
  PartitionedFileRecordBatch,
  PartitioningFileRecordBatcher,
  SingleFileRecordBatch,
  TimePartitioningFilePathFormatter
}
import com.adform.streamloader.util.TimeExtractor
import io.huta.sle.deduplication.DeduplicatingRecordBatchingSink
import io.huta.sle.proto.ComplexTypeOuterClass.ComplexType
import io.huta.sle.extension.{AnnotatedProtoParquetFileBuilder, AnnotatedProtoRecord, GenericRecordFormatter}
import org.apache.hadoop.fs.FileSystem
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.hadoop.conf.Configuration
import java.time.{Duration, LocalDateTime, ZoneId}
import java.util.Properties

object SimpleConfiguration {

  def hadoopFileSystem(): FileSystem = {
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", "hdfs://localhost:9000")
    hadoopConf.set("fs.hdfs.impl.disable.cache", "true")
    FileSystem.get(hadoopConf)
  }

  def kafkaProps(): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "loader-group")
    props.put(ConsumerConfig.DEFAULT_ISOLATION_LEVEL, "read_committed")
    props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "0")
    props
  }

  def recordBatcher(): PartitioningFileRecordBatcher[LocalDateTime, AnnotatedProtoRecord[ComplexType]] = {
    PartitioningFileRecordBatcher
      .builder()
      .recordFormatter(new GenericRecordFormatter[ComplexType])
      .recordPartitioner((r, _) =>
        r.watermark.toDateTime
          .withMinute(0)
          .withSecond(0)
          .withNano(0)
      )
      .fileBuilderFactory(_ =>
        new AnnotatedProtoParquetFileBuilder[ComplexType](
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

  def parseFixedFileCommitStrategy(): FileCommitStrategy.ReachedAnyOf =
    FileCommitStrategy.ReachedAnyOf(
      recordsWritten = Some(6),
      fileOpenDuration = Some(Duration.ofMinutes(5)),
      fileSize = Some(256000000)
    )

  def batchStorage(hadoopFileSystem: FileSystem): HadoopFileStorage[LocalDateTime] = {
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

  def deduplicatingSink(
      fileSystem: FileSystem
  ): DeduplicatingRecordBatchingSink[PartitionedFileRecordBatch[LocalDateTime, SingleFileRecordBatch]] =
    DeduplicatingRecordBatchingSink
      .builder()
      .recordBatcher(recordBatcher())
      .batchStorage(batchStorage(fileSystem))
      .batchCommitQueueSize(1)
      .interval(StreamInterval.OffsetRange(1000))
      .build()

  private def buildSink(
      fileSystem: FileSystem
  ): RecordBatchingSink[PartitionedFileRecordBatch[LocalDateTime, SingleFileRecordBatch]] = RecordBatchingSink
    .builder()
    .recordBatcher(recordBatcher())
    .batchStorage(batchStorage(fileSystem))
    .batchCommitQueueSize(1)
    .build()
}
