package com

import scala.collection.mutable

package object mine {
  /* type MuHashMap[A, B] 和 val MuHashMap 对应 val hashMap: MuHashMap[String, String] = MuHashMap() */
  type MuHashMap = mutable.HashMap[String, String] // 作为类型限定或返回值
  val MuHashMap: mutable.HashMap.type = mutable.HashMap // 作为创建类型

  type MuArrayBuffer[A] = mutable.ArrayBuffer[A]
  val MuArrayBuffer: mutable.ArrayBuffer.type = mutable.ArrayBuffer

  type partialFun = PartialFunction[Any, Unit]
  val partialFun: PartialFunction.type = PartialFunction
}
