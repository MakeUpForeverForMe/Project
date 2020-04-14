package com.mine.flinkKafaka.mysql2kafka.bean

import java.sql.Timestamp

import scala.beans.BeanProperty

class Customer {
    @BeanProperty var keywords: String = _

    override def toString: String = keywords
}
