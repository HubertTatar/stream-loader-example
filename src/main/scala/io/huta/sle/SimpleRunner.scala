package io.huta.sle

import com.adform.streamloader.StreamLoader
import io.huta.sle.config.Configurations
import io.huta.sle.metrics.Metrics

import java.util.concurrent.Executors

object SimpleRunner {
  def main(args: Array[String]): Unit = {
    val registry = Metrics.registry()
    val metricExecutor = Executors.newSingleThreadExecutor()
    val metricServer = Metrics.metricServer(registry, metricExecutor)

    val fileSystem = Configurations.hadoopFileSystem()
    val source = Configurations.kafkaSource(Configurations.kafkaProps(), Seq("greetings_topic"))

    val sink = Configurations.deduplicatingSink(fileSystem)
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

}
