package com.weshare.utils

import com.weshare.pmml.domain.PmmlParam
import org.apache.hadoop.fs.{FileSystem, Path}
import org.jpmml.evaluator.LoadingModelEvaluatorBuilder


/**
 * created by chao.guo on 2021/2/26
 **/
object PmmlUtils {
// var evaluator: ModelEvaluator[_]=null

  /**
   * 初始化 PmmlUtils
    * @param
   * @return
   */

def initPmmUtils(pmmlParam:PmmlParam) ={
  val conf = HdfsUtils.initConfiguration(pmmlParam.hdfs_master,isHa = pmmlParam.isHa)
  val fs = FileSystem.get(conf)
if (!fs.exists(new Path(pmmlParam.pmml_url))) {
  throw new RuntimeException(s"pmml file is not exists!${pmmlParam.pmml_url}")

}
  val stream = fs.open(new Path(pmmlParam.pmml_url))
  //val pmmlIs = getClass.getClassLoader.getResourceAsStream(pmmlFilePath)
  // Create the evaluator
  val evaluator =new LoadingModelEvaluatorBuilder()
      .load(stream)
      .build()
  stream.close()
  evaluator
}

 /* /**
   * 获取输入参数
   * @return
   */
 def getInputFileds() ={
   import scala.collection.JavaConversions._
   val inputFields: util.List[InputField] = evaluator.getInputFields
   inputFields.map(it=>(it.getFieldName.toString,it.getDataType.value())).toMap
 }*/

  /**
   * 获取辅助输出结果
   * @return
   */
  /*def getOutFileds() ={
    import scala.collection.JavaConversions._
    val outputFields = evaluator.getOutputFields
    outputFields.map(it=>(it.getFieldName.toString,it.getDataType.value())).toMap
  }

  /**
   * 获取输出结果
   * @return
   */
  def getTargetFields() ={
    import scala.collection.JavaConversions._
    val outputFields = evaluator.getTargetFields
    outputFields.map(it=>(it.getFieldName.toString,it.getDataType.value())).toMap
  }*/



}
