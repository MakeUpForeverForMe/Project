package com.mine.test.bean

import scala.beans.BeanProperty

class Customer {
    @BeanProperty var id: String = _
    @BeanProperty var cTime: String = _
    @BeanProperty var uTime: String = _
    @BeanProperty var birthDate: String = _
    @BeanProperty var sex: Int = _
    @BeanProperty var city: String = _
    @BeanProperty var expectation: Int = _
    @BeanProperty var province: String = _
    @BeanProperty var sourceChannel: Int = _

    override def toString: String = {
        "id = " + id + "\tcTime = " + cTime + "\tuTime = " + uTime + "\tbirthDate = " + birthDate + "\tsex = " + sex + "\tcity = " + city + "\texpectation = " + expectation + "\tprovince = " + province + "\tsourceChannel = " + sourceChannel
    }
}
