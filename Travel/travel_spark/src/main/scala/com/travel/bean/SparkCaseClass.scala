package com.travel.bean

//通过样例类来封装我们的topic，partition以及offset的值，注意这里需要对offset进行重新排序
case class SparkCaseClass(topic:String, partition:Int, offset:Long) extends Comparable[SparkCaseClass] with Serializable {
  override def compareTo(o: SparkCaseClass): Int = {
    this.offset.compareTo(o.offset)
  }
}


case class HaiKouOrder(order_id:String,city_id:String, starting_lng:String,starting_lat:String) extends Serializable



