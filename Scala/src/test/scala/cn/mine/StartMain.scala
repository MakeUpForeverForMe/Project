package cn.mine

import cn.mine.util.{AesUtils, DateUtils}

import java.util.regex.Pattern
import cn.mine.utils.scala.LoggerUtil.{logLevel, logger, rootLogger}
import cn.mine.utils.scala.UtilsGet.{dataGet, logGet, mobileGet, powGet}
import org.junit.jupiter.api.Test

import scala.math.random
import scala.util.Random


class StartMain {

    rootLogger("info")
    logLevel("fatal", this.getClass)

    @Test
    def test3(): Unit = {
        val a = 'a'
        println('a'.toInt.toChar, a.toInt.toChar)
    }

    @Test
    def test1(): Unit = {
        for (i <- 0 until 1) println(i)
        println("--------")
        for (i <- 0 to 1) println(i)
    }

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
    def test2(): Unit = {
        val list = List(1, 2, 3, 4, 5)
        val str = list.map(_.toString).reduce[String](_ + ":" + _)
        println(str)
    }

    @Test
    def perTest(): Unit = (0 to 9).par.foreach(num => print(num + " "))

    @Test
    def randomTest(): Unit = for (_ <- 1 to 100; ran = (random * 41 + 20).toInt; if ran == 20) logger(ran)

    @Test // log10(163) = 2.2121876044039577
    def logTest(): Unit = logger(logGet(163, 10))

    @Test // 10^2.2121876044039577 = 162.99999999999994 ≈ 163
    def powTest(): Unit = logger(powGet(1, 2.2121876044039577))

    @Test // 2019-08-22 10:33:00
    def dt_2_ft(): Unit = logger("dt_2_ft : " + DateUtils.DATE_TIME_STAMP.toDefaultDateTime("20190822103300"))

    @Test // 20190822103300
    def ft_2_dt(): Unit = logger("ft_2_dt : " + DateUtils.DATE_TIME_LINE.transform("2019-08-22 10:33:00", DateUtils.DATE_TIME_STAMP.formatter))

    @Test // 秘钥：weshare666  AdDesv4O8b9QR5jIZ6hwgw== : 18812345678
    def encryptTest1(): Unit = for (sourceCode <- dataGet("data/source_code.txt")) logger(AesUtils.encrypt(sourceCode, AesUtils.PASSWORD_WESHARE) + " : " + sourceCode)

    @Test // 秘钥：tencentabs123456  WPMGECuw1atPn/FItiuoig== : 18812345678
    def encryptTest2(): Unit = for (sourceCode <- dataGet("data/source_code.txt")) logger(AesUtils.encrypt(sourceCode, AesUtils.PASSWORD_TENCENT) + " : " + sourceCode)

    @Test // WPMGECuw1atPn/FItiuoig== : 18812345678
    def decryptTest(): Unit = for (secretCode <- dataGet("data/secret_code.txt")) logger(secretCode + " : " + AesUtils.decrypt(secretCode, AesUtils.PASSWORD_TENCENT))


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
    def charAtTest(): Unit = {
        val str = "2019-09-27 18:53:00"
        println(str.charAt(0))
        println(Character.isDigit(str.charAt(0)))
    }

    @Test
    def untilOrToTest(): Unit = {
        for (i <- 0 to 10) println(i) // 打印 0 到 10 共 11 个数
        for (i <- 0 until 10) println(i) // 打印 0 到 9 共 10 个数
    }

    @Test
    def testGetDateFormat(): Unit = {
        logger(getDateFormat("2019年09月27日 18时53分00秒")) // yyyy年MM月dd日 HH时mm分ss秒
        logger(getDateFormat("2019-09-27 18:53:00")) // yyyy-MM-dd HH:mm:ss
        logger(getDateFormat("20190927185300")) // yyyyyyyyyyyyyy
    }

    private def getDateFormat(str: String): String = {
        var year = false
        val pattern = Pattern.compile("^[-+]?[\\d]*$")
        if (pattern.matcher(str.substring(0, 4)).matches()) year = true

        val sb = new StringBuilder()
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
        for (i <- 0 until str.length) {
            val chr = str.charAt(i)
            if (Character.isDigit(chr)) {
                index match {
                    case 0 => sb.append("y")
                    case 1 => sb.append("M")
                    case 2 => sb.append("d")
                    case 3 => sb.append("H")
                    case 4 => sb.append("m")
                    case 5 => sb.append("s")
                    case 6 => sb.append("S")
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

    @Test
    def test_1(): Unit = {
        var (a, b) = (1, 2)
        a = a ^ b
        b = a ^ b
        a = a ^ b
        println(a + " " + b)
    }
}
