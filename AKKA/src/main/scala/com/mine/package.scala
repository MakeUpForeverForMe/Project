package com

import scala.collection.mutable

package object mine {
  /* type ImuMap 对应 imuMap: ImuMap ， val ImuMap 对应 ImuMap() 。 即： val imuMap: ImuMap = ImuMap() */
  type ImuMap = Map[String, String] // 作为类型限定或返回值
  val ImuMap: Map.type = Map // 作为创建类型

  type MuArrayBuffer[A] = mutable.ArrayBuffer[A]
  val MuArrayBuffer: mutable.ArrayBuffer.type = mutable.ArrayBuffer

  type partialFun = PartialFunction[Any, Unit]
  val partialFun: PartialFunction.type = PartialFunction
}
