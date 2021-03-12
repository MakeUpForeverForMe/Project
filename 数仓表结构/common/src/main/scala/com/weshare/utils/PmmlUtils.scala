package com.weshare.utils

import org.jpmml.evaluator.LoadingModelEvaluatorBuilder


/**
 * created by chao.guo on 2021/2/26
 **/
object PmmlUtils {
// var evaluator: ModelEvaluator[_]=null

  /**
   * 初始化 PmmlUtils
    * @param pmmlFilePath
   * @return
   */

def initPmmUtils(pmmlFilePath: String) ={
  val pmmlIs = getClass.getClassLoader.getResourceAsStream(pmmlFilePath)
  // Create the evaluator
  val evaluator =new LoadingModelEvaluatorBuilder()
      .load(pmmlIs)
      .build()
    pmmlIs.close()
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
