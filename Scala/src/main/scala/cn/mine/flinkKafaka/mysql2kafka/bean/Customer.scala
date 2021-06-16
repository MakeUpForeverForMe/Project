package cn.mine.flinkKafaka.mysql2kafka.bean

import cn.mine.util.DateUtils

class Customer {
    var id: String = _
    var server_id: String = _
    var black_databases: String = _
    var black_tables: String = _
    var create_time: String = _
    var keywords: String = _
    var cTime: String = _
    var uTime: String = _
    var birthday: String = _
    var sex: Int = _
    var city: String = _
    var expectation: Int = _
    var province: String = _
    var sourceChannel: Int = _

    override def toString: String = {
        "" +
                (if (id != null) id + "\t" else "") +
                (if (server_id != null) server_id + "\t" else "") +
                (if (black_databases != null) black_databases + "\t" else "") +
                (if (black_tables != null) black_tables + "\t" else "") +
                (if (create_time != null) create_time + "\t" else "") +
                (if (keywords != null) keywords + "\t" else "") +
                (if (cTime != null) DateUtils.DATE_TIME_LINE.transform(cTime, DateUtils.DATE_TIME_STAMP.formatter) + "\t" else "") +
                (if (uTime != null) DateUtils.DATE_TIME_LINE.transform(uTime, DateUtils.DATE_TIME_STAMP.formatter) + "\t" else "") +
                (if (birthday != null) birthday + "\t" else "") +
                (if (sex != 0) sex + "\t" else "") +
                (if (city != null) city + "\t" else "") +
                (if (expectation != 0) expectation + "\t" else "") +
                (if (province != null) province + "\t" else "") +
                (if (sourceChannel != 0) sourceChannel + "\t" else "")
    }.trim
}
