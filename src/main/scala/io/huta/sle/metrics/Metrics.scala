package io.huta.sle.metrics

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.micrometer.core.instrument.binder.jvm.{ClassLoaderMetrics, JvmGcMetrics, JvmMemoryMetrics, JvmThreadMetrics}
import io.micrometer.core.instrument.binder.system.{FileDescriptorMetrics, ProcessorMetrics, UptimeMetrics}
import io.micrometer.prometheus.{PrometheusConfig, PrometheusMeterRegistry}

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.ExecutorService

object Metrics {
  def registry(): PrometheusMeterRegistry = {
    val prometheus = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
    List(
      new JvmGcMetrics(),
      new JvmMemoryMetrics(),
      new JvmThreadMetrics(),
      new ClassLoaderMetrics(),
      new UptimeMetrics(),
      new ProcessorMetrics(),
      new FileDescriptorMetrics()
    ).foreach(metric => metric.bindTo(prometheus))
    prometheus
  }

  def metricServer(registry: PrometheusMeterRegistry, executor: ExecutorService): HttpServer = {
    val server = HttpServer.create(new InetSocketAddress(8081), 0)
    server.setExecutor(executor)

    server.createContext(
      "/metrics",
      (exchange: HttpExchange) => {
        val scraped = registry.scrape()
        exchange.sendResponseHeaders(200, scraped.length)
        val responseBody = exchange.getResponseBody
        responseBody.write(scraped.getBytes(StandardCharsets.UTF_8))
        responseBody.flush()
        responseBody.close()
      }
    )

    server
  }
}
