package com.mine

import java.util.regex.Pattern

import com.mine.utils.scala.LoggerUtil.{rootLogger, logLevel, logger}
import com.mine.utils.scala.UtilsGet.{logGet, powGet, dataGet, mobileGet}
import com.mine.utils.java.{AesPlus, DateFormat}

import org.junit.Test

import scala.math.random
import scala.util.Random


class StartMain {

  rootLogger("info")
  logLevel("info", this.getClass)


  @Test
  def test(): Unit = {
    //    val p1 = Array(1, 2, 3)
    //    val p2 = Array("小明", "小红", "小华")
    //    p1.foreach(p => for (i <- 0 until p1.length; if p == p1(i)) println(p1(i) + ", " + p2(i)))
    logger("resource1 => " + this.getClass.getClassLoader.getResource("jdbc/MySQL_JDBC.properties"))
    logger("resource2 => " + this.getClass.getResource(""))
    logger("resource3 => " + this.getClass.getResource("/jdbc/MySQL_JDBC.properties"))
    logger("resource4 => " + this.getClass.getResource("/"))
    //    println("resource5 => " + Source.fromFile(""))
  }

  @Test
  def perTest(): Unit = (1 to 10).par.foreach(num => print(num + " "))

  @Test
  def randomTest(): Unit = for (i <- 1 to 100; ran = (random * 41 + 20).toInt; if ran == 20) logger(ran)

  @Test // log10(163) = 2.2121876044039577
  def logTest(): Unit = logger(logGet(163, 10))

  @Test // 10^2.2121876044039577 = 162.99999999999994 ≈ 163
  def powTest(): Unit = logger(powGet(1, 2.2121876044039577))

  @Test // 2019-08-22 10:33:00
  def dt_2_ft(): Unit = logger("dt_2_ft : " + new DateFormat().dt_2_ft("20190822103300"))

  @Test // 20190822103300
  def ft_2_dt(): Unit = logger("ft_2_dt : " + new DateFormat().ft_2_dt("2019-08-22 10:33:00"))

  @Test // WPMGECuw1atPn/FItiuoig== : 18812345678
  def encryptTest(): Unit = for (sourceCode <- dataGet("data/source_code.txt")) logger(new AesPlus().encrypt(sourceCode) + " : " + sourceCode)

  @Test // WPMGECuw1atPn/FItiuoig== : 18812345678
  def decryptTest(): Unit = for (secretCode <- dataGet("data/secret_code.txt")) logger(secretCode + " : " + new AesPlus().decrypt(secretCode))


  @Test
  def userInfo(): Unit = {
    val provinceGet: List[String] = dataGet("data/province.txt")
    val cityGet: List[String] = dataGet("data/city.txt")

    val amount = powGet(5).toInt
    for (aa <- 0 until amount) {
      val mobile = mobileGet
      val province = provinceGet(Random.nextInt(provinceGet.length))
      val city = cityGet(Random.nextInt(cityGet.length))
      logger(f"$aa%-10s$mobile%11s$province%15s$city%20s")
    }
  }


  @Test
  //  def testgGetDateFormat(): Unit = logger(getDateFormat("2019年09月27日 18时53分00秒"))
  def testgGetDateFormat(): Unit = logger(getDateFormat("2019-09-27 18:53:00"))

  private def getDateFormat(str: String): String = {
    var year = false
    val pattern = Pattern.compile("^[-\\+]?[\\d]*$")
    if (pattern.matcher(str.substring(0, 4)).matches()) {
      year = true
    }
    var sb = new StringBuilder()
    var index = 0
    if (!year) {
      if (str.contains("月") || str.contains("-") || str.contains("/")) {
        if (Character.isDigit(str.charAt(0))) {
          index = 1
        }
      } else {
        index = 3
      }
    }
    for (i: Int <- 0 until str.length) {
      val chr = str.charAt(i)
      if (Character.isDigit(chr)) {
        if (index == 0) {
          sb.append("y")
        }
        if (index == 1) {
          sb.append("M")
        }
        if (index == 2) {
          sb.append("d")
        }
        if (index == 3) {
          sb.append("H")
        }
        if (index == 4) {
          sb.append("m")
        }
        if (index == 5) {
          sb.append("s")
        }
        if (index == 6) {
          sb.append("S")
        }
      } else {
        if (i > 0) {
          val lastChar = str.charAt(i - 1)
          if (Character.isDigit(lastChar)) {
            index += 1
          }
        }
        sb.append(chr)
      }
    }
    sb.toString()
  }
}
