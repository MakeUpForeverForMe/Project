package cn.mine

import java.io.File
import java.net.URL
import java.nio.channels.FileChannel
import java.nio.file.{Paths, StandardOpenOption}

import org.apache.commons.io.FileUtils
import org.junit.Test

class ReadWrite {
  @Test
  def u2l(): Unit = {
    val inURL = "http://10.83.0.129:8999/dtconf/dtconf.zip"
    //    val outURLFile = System.getProperty("user.dir") + "/etc/dtconf.zip"
    val outURLFile = "D:\\Users\\ximing.wei\\Desktop\\dtconf.zip"
    copyURLToFile(inURL, outURLFile)
  }

  @Test
  def l2l(): Unit = {
    val inFile = "D:\\Users\\ximing.wei\\Desktop\\dtconf.zip"
    val outFile = "D:\\Users\\ximing.wei\\Desktop\\dtconf.zip2"
    localFiletToFile(inFile, outFile)
  }

  private def localFiletToFile(inFile: String, outFile: String): Unit = {
    val inChannel = FileChannel.open(Paths.get(inFile), StandardOpenOption.READ)
    val outChannel = FileChannel.open(Paths.get(outFile), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE)
    //    inChannel.transferTo(0, inChannel.size(), outChannel)
    outChannel.transferFrom(inChannel, 0, inChannel.size)
    inChannel.close()
    outChannel.close()
  }

  private def copyURLToFile(inURL: String, outFile: String): Unit = {
    FileUtils.copyURLToFile(new URL(inURL), new File(outFile))
  }
}
