package com.mine

object ApplyTest {
  def main(args: Array[String]): Unit = {
    val myTest = MyTest()
    myTest.myTest()
  }

  object MyTest {
    def apply(): MyTest = new MyTest()
  }

  class MyTest() {
    def myTest(): Unit = {
      println("测试用例")
    }
  }
}
