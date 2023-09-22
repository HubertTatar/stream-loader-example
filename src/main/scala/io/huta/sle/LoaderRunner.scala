package io.huta.sle

import com.adform.streamloader.{BuildInfo, StreamLoader}
import com.adform.streamloader.util.Logging
import com.typesafe.config.{Config, ConfigFactory}
import io.huta.sle.metrics.Metrics
import io.huta.sle.config.ConfigExtensions._
import io.huta.sle.config.ConfigUtil._
import io.huta.sle.config.Configurations
import io.huta.sle.loader.Loader
import io.micrometer.core.instrument.MeterRegistry
import org.apache.hadoop.fs.FileSystem

import java.util.concurrent.{ConcurrentHashMap, Executors}

object LoaderRunner extends Logging {

  private val failedThreads = ConcurrentHashMap.newKeySet[Thread]()

  def main(args: Array[String]): Unit = {
    log.info("Starting loader")
    log.info(s"Using stream loader library version ${BuildInfo.version} (${BuildInfo.gitHeadCommit.get})")

    Thread.setDefaultUncaughtExceptionHandler((thread: Thread, throwable: Throwable) => {
      log.error(throwable)(s"Thread '${thread.getName}' terminated unexpectedly, exiting")
      failedThreads.add(thread) // so that we don't wait for it in the shutdown hook
      System.exit(1)
    })

    val cfg: Config = ConfigFactory.load().getConfig("stream-loader")
    printConfig(cfg)

    val metricRegistry = Metrics.registry()
    val metricExecutor = Executors.newSingleThreadExecutor()
    val metricServer = Metrics.metricServer(metricRegistry, metricExecutor)

    val consumerProps = cfg.getConfig("kafka.consumer").toProperties
    val topics = cfg.getString("topics").split(",").toSeq
    val source = Configurations.kafkaSource(consumerProps, topics)

    val fileSystem = Configurations.hadoopFileSystem()

    val loader = Class
      .forName(cfg.getString("loader.clazz"))
      .getConstructor(classOf[FileSystem], classOf[Config], classOf[MeterRegistry])
      .newInstance(fileSystem, cfg, metricRegistry)
      .asInstanceOf[Loader]

    Thread.currentThread().setName("main-thread")

    val consumerThreads = for (i <- 1 to cfg.getInt("consumer-threads")) yield {
      val streamLoader = new StreamLoader(source, loader.buildSink())
      streamLoader.setMetricRegistry(metricRegistry)
      val thread = new Thread(() => streamLoader.start(), s"loader-$i")
      streamLoader -> thread
    }

    Runtime.getRuntime.addShutdownHook(
      new Thread(
        () => {
          log.info("Caught shutdown hook, stopping all loaders and waiting for threads to finish")
          consumerThreads.foreach { case (loader, _) => loader.stop() }
          consumerThreads.collect {
            case (_, thread) if thread.isAlive && !failedThreads.contains(thread) => thread.join()
          }
          metricServer.stop(1000)
          loader.close()
        },
        "shutdown-thread"
      )
    )

    log.info("Starting loader threads")
    consumerThreads.foreach { case (_, thread) => thread.start() }
  }

}
