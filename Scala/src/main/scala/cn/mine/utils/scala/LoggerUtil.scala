package cn.mine.utils.scala

import org.apache.log4j.{Level, Logger}

import scala.reflect.ClassTag

object LoggerUtil extends LoggerUtil

sealed class LoggerUtil {

  def rootLogger(level: String = "all"): Unit = Logger.getRootLogger.setLevel(caseLevel(level))

  private var message: String = ""

  private var level: String = "all"
  private var clazz: Class[_] = this.getClass

  def logLevel(logLevel: String, clazz: Class[_]): Unit = {
    this.level = logLevel
    this.clazz = clazz
  }

  def logger[T: ClassTag](msg: T, level: String = level, t: Throwable = null): Unit = Logger.getLogger(clazz).log(caseLevel(level), msg + f"$message", t)


  private final def caseLevel(level: String): Level = level.toLowerCase match {
    case "all"   => message = ""; Level.ALL
    case "trace" => message = ""; Level.TRACE
    case "debug" => message = ""; Level.DEBUG
    case "info"  => message = ""; Level.INFO
    case "warn"  => message = ""; Level.WARN
    case "error" => message = ""; Level.ERROR
    case "fatal" => message = ""; Level.FATAL
    case "off"   => message = ""; Level.OFF
    case _       => message = f"\n输入错误 -- $level\n应为如下选项 : all、trace、debug、info、 warn、 error、 fatal、 off \n"; Level.FATAL
  }
}
