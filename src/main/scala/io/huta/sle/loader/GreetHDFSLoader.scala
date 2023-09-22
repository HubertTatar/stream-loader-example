package io.huta.sle.loader
import com.typesafe.config.Config
import io.micrometer.core.instrument.MeterRegistry
import io.huta.sle.proto.Greet.GreetRequest
import org.apache.hadoop.fs.FileSystem

class GreetHDFSLoader(fileSystem: FileSystem, config: Config, metricRegistry: MeterRegistry)
    extends HDFSLoader[GreetRequest](fileSystem, config, metricRegistry) {}
