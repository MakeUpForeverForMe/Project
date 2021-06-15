package com.mine.test.bean


class Customer {
    var id: String = _
    var cTime: String = _
    var uTime: String = _
    var birthDate: String = _
    var sex: Int = _
    var city: String = _
    var expectation: Int = _
    var province: String = _
    var sourceChannel: Int = _

    override def toString: String = {
        "id = " + id + "\tcTime = " + cTime + "\tuTime = " + uTime + "\tbirthDate = " + birthDate + "\tsex = " + sex + "\tcity = " + city + "\texpectation = " + expectation + "\tprovince = " + province + "\tsourceChannel = " + sourceChannel
    }
}
