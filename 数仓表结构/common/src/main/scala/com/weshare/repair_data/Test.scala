package com.weshare.repair_data

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.weshare.pmml.domain.PmmlMode
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.{SparkSession, functions}

import scala.collection.mutable.ListBuffer
import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

/**
 * created by chao.guo on 2020/11/26
 **/
object Test {
  def main(args: Array[String]): Unit = {

    /* val sparkSession = SparkSession.builder()
       .appName("MockDataAndInvokePmml" + System.currentTimeMillis())
       .config("hive.execution.engine","spark")
       .config("hive.exec.dynamic.partition","true")
       .config("hive.exec.dynamic.partition.mode","nonstrict")
       .config("hive.exec.max.dynamic.partitions","100000")
       .config("hive.exec.max.dynamic.partitions.pernode","100000")
       .config("hive.exec.max.created.files","100000")
       .config("hive.merge.size.per.task","268435456")
       .config("hive.merge.mapredfiles","true")
       .config("hive.merge.smallfiles.avgsize","268435456")
       .config("mapreduce.input.fileinputformat.split.minsize","268435456")
       .config("spark.sql.shuffle.partitions","100")

         .master("local[4]")
          .config("hive.metastore.uris", "thrift://10.83.0.47:9083")
       .enableHiveSupport()
       .getOrCreate()

   //sparkSession.sql("add jar ")
   sparkSession.sql("CREATE TEMPORARY FUNCTION row_sequence AS 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence' USING jar 'hdfs://node5:8020/////user/root/hive-contrib-2.1.1-cdh6.3.0.jar'")
     val originDs = sparkSession.sql(
       s"""
          | select *
          | from
          | (
          | select *,row_sequence() as rn
          | from
          |  eagle.one_million_random_risk_data where cycle_key = '0' and
          |  project_id='WS0006200001' and biz_date='2020-11-30'
          | ) tmp
          | where tmp.rn <=10
          |

        """.stripMargin)
     println(originDs.toJavaRDD.getNumPartitions)
     originDs.show(10,false)
     originDs.count()*/

    val list = List(0.8455011481157746,0.021004146024571353,0.00759314996536805,0.12590155589428598)
    println(Random.shuffle(list))

    /*doubles.foreach(it=>{
      println(Math.pow(ThreadLocalRandom.current().nextDouble(1.000), 1 / it))
    })


*/
    /*val basenum=10
    var result_list = ListBuffer[Double]()
    list.foreach(it=>{
      val num = (BigDecimal.apply(it)*basenum).toInt
      for (a <- 1 to num){
        result_list+=it
      }
    })
    println(result_list)
    for (elem <- 0.to(10)) {
      println(result_list(Math.abs(Random.nextInt(result_list.size))))
    }*/




    /*val sorted = doubles.sorted.reverse
    println(sorted)
      val d = Random.nextGaussian()
    println(d)*/






    // }

    val format = new SimpleDateFormat("yyyy-MM-dd")
    /**
    val date = DateUtils.round(format.parse("2021-03-27"),Calendar.MONTH)
    println(format.format(date))
    println(date)*/
    /*  val date="2021-04-14"
    val current_date = format.parse(date)
    val cal = Calendar.getInstance()
    cal.setTime(current_date)
    val current_month = Calendar.getInstance()
    current_month.setTime(format.parse(date))
    current_month.add(Calendar.MONTH,1)
    current_month.set(Calendar.DAY_OF_MONTH,1)
    current_month.add(Calendar.DAY_OF_MONTH,-1)
    println(format.format(current_month.getTime))
    println((cal.get(Calendar.DAY_OF_YEAR) - current_month.get(Calendar.DAY_OF_YEAR)))
    if((cal.get(Calendar.DAY_OF_YEAR)-current_month.get(Calendar.DAY_OF_YEAR))< -5){
      println(1)
    }*/



    /*println(calculationMonth("2021-04-15", "2021-09-19", 6))*/


  }


  val calculationMonth=(loan_active_date:String,project_end_date:String,loan_init_term:BigDecimal)=>{
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val loan_active_ = simpleDateFormat.parse(loan_active_date)
    val calendar = Calendar.getInstance()
    calendar.setTime(loan_active_)
    calendar.add(Calendar.MONTH,loan_init_term.toInt)
    val calendar2 = Calendar.getInstance()
    val project_end_ = simpleDateFormat.parse(project_end_date)
    calendar2.setTime(project_end_)
    if(calendar.getTime.compareTo(calendar2.getTime)>0){
      calendar.set(Calendar.DAY_OF_MONTH,ThreadLocalRandom.current().nextInt(calendar2.get(Calendar.DAY_OF_MONTH)))
    }
    calendar.add(Calendar.MONTH,-loan_init_term.toInt)
    simpleDateFormat.format(calendar.getTime)
  }




  def randomDateOfThisMonth(dateStr : String) : String = {
    val yearMonthFormat = new SimpleDateFormat("yyyy-MM-")
    val str = yearMonthFormat.format(yearMonthFormat.parse(dateStr))
    val month = str.split("-")(1)
    var randomDay : String = null
    var random : Int = 0
    if("02" .equals(month) ) {
      random = Random.nextInt(27) + 1
      randomDay = if (random < 10) "0" + random.toString else random.toString
    } else {
      random = Random.nextInt(30) + 1
      randomDay = if (random < 10) "0" + random.toString else random.toString
    }
    str + randomDay
  }
  /* val fields = classOf[PmmlMode].getDeclaredFields
       val fields_str = fields.map(it => {
         "\""+it.getName+"\""
       }).toList.mkString(",")

       println(fields_str)
*/
  /*val lists = generateRandomPrincipal(BigDecimal(10000), 20)
  println(lists)

}

def generateRandomPrincipal(mockRemainPrincipal: BigDecimal, billCounts: Int) = {
  val factor = 1
  var array = new Array[BigDecimal](billCounts)
  val avgPrincipal =  ((mockRemainPrincipal / billCounts) * factor).setScale(0,RoundingMode.FLOOR)
  //val random=((mockRemainPrincipal / billCounts) * (1-factor)).setScale(0,RoundingMode.FLOOR).toInt
  //array.map(_=>avgPrincipal+BigDecimal(Random.nextInt(random))).toList
  array.map(_=>avgPrincipal).toList
  //回收数组长度

  /*val factor = 0.5
  val array = new Array[BigDecimal](billCounts)
  val avgPrincipal =  ((mockRemainPrincipal / billCounts) * factor).setScale(0,RoundingMode.FLOOR)
  for (i <- 0 until billCounts) {
    array(i) = 10
  }
  //为了取整,重新计算虚拟剩余本金
  val newMockRemainPrincipal = (avgPrincipal  * billCounts) / factor
  val cycleCounter = (newMockRemainPrincipal / avgPrincipal) - BigDecimal(billCounts)
  for (i <- 1 to cycleCounter.toInt) {
    //随机选中第1到总条数中的某一条数据,给他加上分配金额
    val randomIndex = Random.nextInt(billCounts)
    array(randomIndex) = array(randomIndex) + avgPrincipal
  }*/
  //array.toList
}*/
}
