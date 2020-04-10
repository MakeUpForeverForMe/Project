package com.mine

import org.junit.Test

class MySQLConsoleTest {

    @Test
    def aaTest(): Unit = {
        Array("CA:California", "WA:Washington", "OR:Oregon").
                map(s => s.split(":")).
                map { case Array(f1, f2) => (f1, f2) }.foreach(println)
    }
}
