package io.huta.sle.loader
import com.typesafe.config.Config
import io.huta.sle.proto.ComplexTypeOuterClass.ComplexType
import org.apache.hadoop.fs.FileSystem

class ComplexTypeHDFSLoader(fileSystem: FileSystem, config: Config) extends HDFSLoader[ComplexType](fileSystem, config) {}
