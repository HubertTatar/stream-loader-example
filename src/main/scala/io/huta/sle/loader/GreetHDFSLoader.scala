package io.huta.sle.loader
import com.typesafe.config.Config
import io.huta.sle.proto.Greet.GreetRequest
import io.micrometer.core.instrument.MeterRegistry
import org.apache.hadoop.fs.FileSystem

class GreetHDFSLoader(fileSystem: FileSystem, config: Config) extends HDFSLoader[GreetRequest](fileSystem, config) {}
