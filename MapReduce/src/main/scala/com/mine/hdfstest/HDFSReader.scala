package com.mine.hdfstest

import java.net.URI

import com.mine.hdfs.ConfProperty
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable
import scala.io.Source

object HDFSReader {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "hdfs")
        val hashMap: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]

        val file = "/tmp/tmp/tdid_name/part-r-00000"

        Source.fromInputStream(FileSystem.get(ConfProperty("conf.properties").conf)
                .open(new Path(URI.create(file))))
                .getLines
                .foreach(item => hashMap.put(item.split("\t")(0), hashMap.getOrElseUpdate(item.split("\t")(0), 0) + 1))

        hashMap.keys.foreach(key => println(key + "\t" + hashMap(key)))
    }
}
