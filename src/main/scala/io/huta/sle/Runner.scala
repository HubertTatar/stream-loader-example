package io.huta.sle

import com.adform.streamloader.StreamLoader
import com.adform.streamloader.sink.batch.RecordBatchingSink
import com.adform.streamloader.sink.file.{PartitionedFileRecordBatch, SingleFileRecordBatch}
import org.apache.hadoop.fs.FileSystem

import java.time.LocalDateTime
import java.util.concurrent.Executors

object Runner {
  def main(args: Array[String]): Unit = {
    val registry = Metrics.registry()
    val metricExecutor = Executors.newSingleThreadExecutor()
    val metricServer = Metrics.metricServer(registry, metricExecutor)

    val fileSystem = Configurations.hadoopFileSystem()
    val source = Configurations.kafkaSource()

    val sink = buildSink(fileSystem)
    val loader = new StreamLoader(source, sink)

    loader.setMetricRegistry(registry)

    sys.addShutdownHook {
      loader.stop()
      fileSystem.close()
      metricServer.stop(10)
    }

    metricServer.start()
    loader.start()
  }

  private def buildSink(
      fileSystem: FileSystem
  ): RecordBatchingSink[PartitionedFileRecordBatch[LocalDateTime, SingleFileRecordBatch]] = RecordBatchingSink
    .builder()
    .recordBatcher(Configurations.recordBatcher())
    .batchStorage(Configurations.batchStorage(fileSystem))
    .batchCommitQueueSize(1)
    .build()
}
