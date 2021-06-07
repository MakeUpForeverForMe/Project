package com.weshare.data_check.Handler

/**
 * created by chao.guo on 2020/10/15
 **/
object HtmlPageHandler {

val html_head ="""
  <!DOCTYPE html>
    <html lang='en'>
      <head>
        <meta charset='UTF-8'>
          <meta name='viewport' content='width=device-width, initial-scale=1.0'>
            <meta http-equiv='X-UA-Compatible' content='ie=edge'>
              <title>Document</title>
            </head>

            <body>
              <style type='text/css'>
                body {
                margin: 20px;
                font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;
  font-size: 12px;
  }

  .style1 {
  width: 800px;
  height: 140px;
  margin: 0px;
  margin-bottom: 20px;
  border: 1px solid #96C2F1;
  word-wrap:break-word;
  display: none
  }
  .h6-button {
  margin-bottom: 10px;
  cursor: pointer;
  }
  .btnDisplay {
  width: 800px;
  height: 140px;
  margin: 0px;
  margin-bottom: 20px;
  border: 1px solid #96C2F1;
  word-wrap:break-word;
  display: block
  }

  table.gridtable {
  font-family: verdana, arial, sans-serif;
  font-size: 11px;
  color: #333333;
  border-width: 1px;
  border-color: #666666;
  border-collapse: collapse;
  }

  table.gridtable th {
  border-width: 1px;
  padding: 8px;
  border-style: solid;
  border-color: #666666;
  background-color: #dedede;
  }

  table.gridtable td {
  border-width: 1px;
  padding: 8px;
  border-style: solid;
  border-color: #666666;
  background-color: #ffffff;
  }
  </style>
"""

val table_pre =
  """
    |
    |  <table class='gridtable'>
    |
    |""".stripMargin

val table_end=
  """
    |
    | </table>
    |
    |""".stripMargin

  val title =
    """
      |<h5>jobName:<span>放款笔数对照</span></h5>
      |
      |
      |
      |""".stripMargin


var th=
  """
    |<tr>
    |            <th>product_code</th>
    |            <th>sum(pr)</th>
    |            <th>sum(int)</th>
    |            <th>count(due_bill_no)</th>
    |			        <th>结果</th>
    |			        <th collapse='2'>sql</th>
    |</tr>
    |
    |
    |""".stripMargin





val tr =
  """
    |<tr>
    |            <td>001801</td>
    |            <td>20.0</td>
    |            <td>300.00</td>
    |            <td>10</td>
    |			<td><button class='h6-button' onclick='threeFn('button1')'>检验sql</button></td>
    |			<td collapse='2'> <div class='style1' id='style1'>
    |			select
    |			loan.product_code,sum(if(bnp_type='Pricinpal',repay_amt,0)),sum(if(bnp_type='Interest',repay_amt,0)),count(distinct
    |			repay_hst.due_bill_no) from (select * from ods.ecas_repay_hst where d_date='2020-10-12' and txn_date=
    |			'2020-10-12' and term>0) repay_hst inner join (select due_bill_no,product_code from ods.ecas_loan where
    |			d_date='2020-10-12')loan on repay_hst.due_bill_no = loan.due_bill_no group by product_code
    |
    |			</div>
    |	     </td>
    |        </tr>
    |		<tr>
    |            <td>001801</td>
    |            <td>20.0</td>
    |            <td>300.00</td>
    |            <td>10</td>
    |			<td><button class='h6-button' onclick='threeFn('button2')'>检验sql</button></td>
    |			<td collapse='2'> <div class='style1' id='style1'>
    |			select
    |			loan.product_code,sum(if(bnp_type='Pricinpal',repay_amt,0)),sum(if(bnp_type='Interest',repay_amt,0)),count(distinct
    |			repay_hst.due_bill_no) from (select * from ods.ecas_repay_hst where d_date='2020-10-12' and txn_date=
    |			'2020-10-12' and term>0) repay_hst inner join (select due_bill_no,product_code from ods.ecas_loan where
    |			d_date='2020-10-12')loan on repay_hst.due_bill_no = loan.due_bill_no group by product_code
    |
    |			</div>
    |	     </td>
    | </tr>
    |""".stripMargin






var footer=
  """
    |</body>
    |<script type='text/javascript'>
    |    var btn = document.getElementsByClassName('h6-button')
    |    var style1 = document.getElementById('style1')
    |    var flag = false
    |
    |    function threeFn() {
    |    if(flag){
    |      flag = false
    |      document.getElementById('style1').className = 'style1';
    |    }else{
    |      flag = true
    |      document.getElementById('style1').className = 'btnDisplay';
    |    }
    |
    |
    |    }
    |  </script>
    |  </html>
    |""".stripMargin


    def mergeHtmlTest(): String ={
      s"""
        |${html_head}
        |${title}
        |${table_pre}
        |${th}
        |${tr}
        |${table_end}
        |${footer}
        |""".stripMargin

    }
//初始化模版信息数据
  def initHtmlPage(job_name:String,batch_date:String,data:List[Map[String,String]],th_arrays:Array[String]): String ={
    if(data.filter(it=>{!it.isEmpty}).length>0) {
      val th_str = getThStr(th_arrays)
      val tr_str = getTrString(data,th_arrays)
      s"""
         |${html_head}
         |
         |<h5>taskName:<span>${job_name}</span><span>批次日期:${batch_date}</span></h5>
         |${table_pre}
         |${th_str}
         |${tr_str}
         |${table_end}
         |
         |""".stripMargin
    }else{
      ""
    }


}



  /**
   *
   * @param taskName job名称
   * @param batch_date 批次时间
   * @param data  数据
   * @param th_arrays 表头
   */
  def initTaskHtmlStr(taskName:String,batch_date:String,data:List[Map[String,String]],th_arrays:Array[String],task_sql:String,task_check_sql:String,mode_name:String): String ={



    if(data.filter(it=>{!it.isEmpty}).length>0) {
      val task_sql_table=
        s"""
           |
           |<table class='gridtable'>
           |<tr>
           |<th>taskSQL</th>
           |<th>taskcheckSQL</th>
           |</tr>
           |<tr>
           |<td>
           | ${task_sql}
           |
           |</td>
           |<td>
           |${task_check_sql}
           |</td>
           |</tr>
           |</table>
           |<br/>
           |""".stripMargin

      val th_str = getThStr(th_arrays)
      val tr_str = getTrString(data,th_arrays)

    s"""
       |${html_head}
       |
       |<h5>模块：${mode_name}<span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;任务名称：${taskName}</span><span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; 批次日期:${batch_date}</span></h5>
       |${task_sql_table}
       |${table_pre}
       |${th_str}
       |${tr_str}
       |${table_end}
       |
       |""".stripMargin
  }else{
    ""
  }

}

  /**
   * 根据数据集返回Th
   * @param th_arrays
   * @return
   */
  def getThStr(th_arrays:Array[String]):String={
    val th_str = new  StringBuffer
    th_str.append("<tr>")
    for (elem <- th_arrays) {
      th_str.append( s"""
                        | <th>${elem}</th>
                        |""".stripMargin)
    }
    //      th_str.append("<th>结果</th><th collapse = '2'>sql</th>")
    th_str.append("</tr>")
    th_str.toString

  }


  /**
   * 根据数据集返回tr
   * @param data
   * @param th_arrays
   * @return
   */
  def getTrString(data:List[Map[String,String]],th_arrays:Array[String]):String={
    val tr_str = new  StringBuffer
    var i = 1
    data.foreach(map=>{
      tr_str.append("<tr>")
      th_arrays.foreach(
        field_name=>{
          tr_str.append(
            s"""
               |
               |<td>${map.getOrElse(field_name,"")}</td>
               |
               |
               |""".stripMargin)
        }
      )
      tr_str.append(
        s"""
           | </tr>
           |""".stripMargin)
      i=i+1
    })
    tr_str.toString


  }





}
