package io.huta.sle

import org.apache.hadoop.fs.Path
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ConfigurationsTest extends AnyFunSpec with Matchers {

  ignore("should connect to HDFS - docker compose is up") {
    val hadoopFileSystem = Configurations.hadoopFileSystem()
    val test = hadoopFileSystem.exists(new Path("/"))
    hadoopFileSystem.close();

    test shouldEqual true
  }
}