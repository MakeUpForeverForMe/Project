package cn.mine.testabc

object TestABC {
    /**
      * 加载顺序
      *
      * 父类-静态属性
      * 父类-静态代码块
      * 子类-静态属性
      * 子类-静态代码块
      * 父类-非静态属性
      * 父类-非静态代码块
      * 父类-构造器
      * 子类-非静态属性
      * 子类-非静态代码块
      * 子类-构造器
      */
    def main(args: Array[String]): Unit = {
        val a = new A("testA")
        println(a)
    }

    object A {
        def apply: A = new A()
    }

    class A {

        var a: String = _

        def this(a: String) = {
            this
            println("a")
            this.a = a
        }

        private val b = new B(a)
        println(b)
    }

    object B {
        def apply: B = new B()
    }

    class B {

        var b: String = _

        def this(b: String) = {
            this
            println("b")
            this.b = b
        }

        private val c = new C(b)
        println(c)
    }

    object C {
        def apply: C = new C()
    }

    class C {

        var c: String = _

        def this(a: String) = {
            this
            println("c")
            this.c = c
        }
    }

}
