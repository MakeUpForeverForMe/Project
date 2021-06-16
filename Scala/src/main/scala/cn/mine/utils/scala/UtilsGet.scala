package cn.mine.utils.scala

import scala.io.Source
import scala.math._


object UtilsGet extends UtilsGet

sealed class UtilsGet {
  /**
    * 获取幂函数信息
    *
    * @param initNum 输入的初始变量
    * @param power   输入的幂变量
    * @param baseNum 输入的基础变量
    * @return 返回值： initNum * baseNum^power^
    * @example 2 * 10^2^ = 2 * 100 = 200
    * @example 0.2 * 10^1.1^ = 0.2 * 12.5892 ≈ 2.51784
    */
  def powGet(initNum: Double = 0, power: Double = 0, baseNum: Double = 10): Double = initNum * pow(baseNum, power)

  /**
    * 获取对数函数信息
    *
    * @param initNum 输入的初始变量
    * @param baseNum 只能输入10或E(默认为E)
    * @return 返回值 log(initNum)
    */
  def logGet(initNum: Double = 0, baseNum: Double = E): Any = if (baseNum == E || baseNum == 10) if (baseNum == E) log(initNum) else log10(initNum) else "请重新输入(只能是E或10)"

  /**
    * 获取学历信息
    *
    * @return 返回String类型的数组（"无学历", "小学", "初中", "中职", "高中", "职高", "技校", "专科", "本科", "硕士", "博士"）
    */
  def eduLevelGet: Array[String] = Array("无学历", "小学", "初中", "中职", "高中", "职高", "技校", "专科", "本科", "硕士", "博士")

  /**
    * 获取婚姻状况信息
    *
    * @return 返回String类型的数组（"未婚", "已婚", "离异", "丧偶"）
    */
  def marryGet: Array[String] = Array("未婚", "已婚", "离异", "丧偶")

  /**
    * 获取11位手机号
    *
    * @return 返回字符串类型的11位手机号(Int的存储长度不够，所以用String)
    */
  def mobileGet: String = 1 + f"${(random * 1000000000).toInt}%-10d".replaceAll(" ", "0")

  /**
    * 输入文件名称，获取文件中的每行数据
    *
    * @param fileName 输入的文件名称
    * @return 返回文件中的每一行数据
    */
  def dataGet(fileName: String): List[String] = Source.fromFile(getClass.getClassLoader.getResource(fileName).getPath).getLines().toList
}
