package io.huta.sle

import com.adform.streamloader.StreamLoader
import io.huta.sle.config.{KafkaSources, SimpleConfiguration}
import io.huta.sle.metrics.Metrics

import java.util.concurrent.Executors

object SimpleRunner {
  def main(args: Array[String]): Unit = {
    val registry = Metrics.registry()
    val metricExecutor = Executors.newSingleThreadExecutor()
    val metricServer = Metrics.metricServer(registry, metricExecutor)

    val fileSystem = SimpleConfiguration.hadoopFileSystem()
    val source = KafkaSources.kafkaSource(SimpleConfiguration.kafkaProps(), Seq("greetings_topic"))

    val sink = SimpleConfiguration.buildSink(fileSystem)
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
