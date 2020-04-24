package com.mine.utils.scala

import java.io.FileWriter

import com.mine.aesutil.AesPlus
import com.mine.dateutil.DateFormat
import com.mine.utils.scala.UtilsGet._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object dmUserInfo {


    // 读取本地文件
    private val path = "D:\\Users\\ximing.wei\\Desktop\\dm_user_info.csv"
    private val writeLocal = new FileWriter(path)
    // 存储数据
    private val detail = new mutable.ListBuffer[String]()
    // 生成数据的数量
    private var amount = 5 * 10 * 10 * 10 * 10 * 10
    // 性别
    private val sexs = Array(1, 2)
    private val haves = Array(0, 1, 2)
    private val isOrNos = Array("是", "否")
    // 限定年龄在 20 - 60 之间
    private val ageBetween = Map("min" -> 20, "max" -> 60)
    // 省份
    private val provinces = dataGet("data/province.txt")
    // 城市
    private val citys = dataGet("data/city.txt")
    private val marital_status = marryGet
    private val education_levels = eduLevelGet
    private val careers = Array("医生", "文员", "IT", "学生", "工程师")
    private val positions = Array("经理", "文员", "IT", "学生", "工程师")
    private val titles = Array("高级工程师", "主任医师", "助理行政", "学生")
    private val principal_states = Array("正常", "逾期", "违约", "已核销", "已结清")
    private val asset_types = Array("个人消费贷款", "汽车抵押贷款", "住房抵押贷款")


    def produce(): ListBuffer[String] = {
        val random = new Random()
        // 生成年龄
        for (amount <- 0 until amount) {
            val code = random.nextInt(1000)
            val num = f"$code%3d".replaceAll(" ", "0")
            val idcard = random.nextLong().abs.toString.slice(0, 18)
            val mobile = random.nextLong().abs.toString.slice(0, 11)
            val births = random.nextLong().abs.toString.slice(0, 14)

            val fushionId = "weshare_" + f"$amount%3d".replaceAll(" ", "0")
            val idcard_aes = AesPlus.encrypt(f"$idcard%18s", AesPlus.PASSWORD_TENCENT)
            val moblie_aes = AesPlus.encrypt(f"$mobile%11s", AesPlus.PASSWORD_TENCENT)
            val name_aes = AesPlus.encrypt(f"$mobile%11s", AesPlus.PASSWORD_TENCENT)
            val birth = DateFormat.dt_2_ft(births)
            val date = births.slice(0, 8)
            val sex = sexs(random.nextInt(sexs.length))
            val city = citys(random.nextInt(citys.length))
            val expectation = random.nextInt(1000000)
            val age = random.nextInt(ageBetween("max") - ageBetween("min") + 1) + ageBetween("min")
            val province = provinces(random.nextInt(provinces.length))
            val have = haves(random.nextInt(haves.length))
            val marital_statu = marital_status(random.nextInt(marital_status.length))
            val education_level = education_levels(random.nextInt(education_levels.length))
            val isOrNo = isOrNos(random.nextInt(isOrNos.length))
            val career = careers(random.nextInt(careers.length))
            val position = positions(random.nextInt(positions.length))
            val title = titles(random.nextInt(titles.length))
            val principal_state = principal_states(random.nextInt(principal_states.length))
            val asset_type = asset_types(random.nextInt(asset_types.length))

            detail += fushionId + "," +
                    idcard_aes + "," +
                    moblie_aes + "," +
                    "client_id_" + num + "," +
                    name_aes + "," +
                    "0.0.0.0" + "," +
                    birth + "," +
                    sex + "," +
                    city + "," +
                    birth + "," +
                    expectation + "," +
                    age + "," +
                    province + "," +
                    sex + "," +
                    have + "," +
                    have + "," +
                    have + "," +
                    have + "," +
                    "cust_code_" + num + "," +
                    "20000" + "," +
                    marital_statu + "," +
                    age + "," +
                    education_level + "," +
                    education_level + "," +
                    isOrNo + "," +
                    sex + "," +
                    "广东省深圳市" + "," +
                    "广东省" + "," +
                    "深圳市" + "," +
                    "广东省深圳市唐宁街18号" + "," +
                    career + "," +
                    position + "," +
                    title + "," +
                    "新分享" + "," +
                    "0571-0808118" + "," +
                    "广东省深圳市中州大厦29楼" + "," +
                    "serial_number_" + num + "," +
                    isOrNo + "," +
                    "idfv_" + num + "," +
                    "idfa_" + num + "," +
                    "imei_" + num + "," +
                    "wx_openid_" + num + "," +
                    "wx_unionid_" + num + "," +
                    "gen_id_" + num + "," +
                    isOrNo + "," +
                    isOrNo + "," +
                    isOrNo + "," +
                    isOrNo + "," +
                    isOrNo + "," +
                    isOrNo + "," +
                    isOrNo + "," +
                    isOrNo + "," +
                    "pri_fusion_id_" + num + "," +
                    num + "," +
                    "source_org_name_" + city + "," +
                    isOrNo + "," +
                    num + "," +
                    "recommend_name_" + city + "," +
                    num + "," +
                    "asset_org_name_" + city + "," +
                    "asset_code_" + num + "," +
                    "asset_name_" + city + "," +
                    isOrNo + "," +
                    "80000" + "," +
                    "100000" + "," +
                    principal_state + "," +
                    moblie_aes + "," +
                    isOrNo + "," +
                    isOrNo + "," +
                    "agency_id_" + num + "," +
                    "project_id" + num + "," +
                    "contract_code_" + num + "," +
                    asset_type + "," +
                    num + "," +
                    asset_type + "," +
                    asset_type + "," +
                    asset_type + "," +
                    asset_type + "," +
                    asset_type + "," +
                    asset_type + "," +
                    asset_type + "," +
                    asset_type + "," +
                    "80000" + "," +
                    "100000" + "," +
                    "12" + "," +
                    "5" + "," +
                    "500" + "," +
                    "8" + "," +
                    isOrNo + "," +
                    birth + "," +
                    birth + "," +
                    expectation + "," +
                    age + "," +
                    num + "," +
                    age + "," +
                    "80000" + "," +
                    "100000" + "," +
                    "250000" + "," +
                    "29000" + "," +
                    "30000" + "," +
                    "12" + "," +
                    "5" + "," +
                    "500" + "," +
                    "8" + "," +
                    sex + "," +
                    have + "," +
                    have + "," +
                    have + "," +
                    have + "," +
                    birth + "," +
                    birth + "," +
                    num + "," +
                    "20190911" +
                    "\n"
        }
        detail
    }


    def main(args: Array[String]): Unit = {
        detail.clear()
        produce().foreach(writeLocal.write)
        writeLocal.flush()
        writeLocal.close()
        //    produce().foreach(print)
    }
}
