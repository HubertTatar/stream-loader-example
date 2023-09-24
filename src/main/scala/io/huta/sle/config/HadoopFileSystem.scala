package io.huta.sle.config

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

object HadoopFileSystem {
  def hadoopFileSystem(hdfsUrl: String): FileSystem = {
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", hdfsUrl)
    hadoopConf.set("fs.hdfs.impl.disable.cache", "true")
    FileSystem.get(hadoopConf)
  }

}
