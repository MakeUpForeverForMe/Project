package com.travel.utils

import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang3.StringUtils


object RowkeyUtil {


  def getRowKey(str:String,numRegion:Int):String ={
    val result: Int = (str.hashCode & Integer.MAX_VALUE) % numRegion
    val prefix:String = StringUtils.leftPad(result+"",4,"0");
    val suffix: String = DigestUtils.md5Hex(str).substring(0,12)
    prefix + suffix
  }




  def main(args: Array[String]): Unit = {
    val str: String = getRowKey("1_20200120180752",8)
    println(str)
  }

}
