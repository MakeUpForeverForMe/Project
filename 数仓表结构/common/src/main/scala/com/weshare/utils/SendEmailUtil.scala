package com.weshare.utils

/**
 * created by chao.guo on 2020/8/29
 *
 **/
import java.util.{Date, Properties}

import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Message, Session, Transport}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object SendEmailUtil {


  var session:Session =_;
  var pro:Properties= new  Properties();
  var transport:Transport=_;

  def initEmailSession(configPath:String,hdfs_master:String,isHa:Int) ={
    val conf = HdfsUtils.initConfiguration(hdfs_master,isHa)
    val inputSteam = FileSystem.get(conf).open(new Path(configPath))
    pro.load(inputSteam)
    inputSteam.close()
    session = Session.getInstance(pro)
    transport = session.getTransport
  }



  /** 发送邮件信息
   *
   * @param
   * @param subjectType
   */
  def  sendMessage(messag:String,subjectType:String): Unit ={
    val senderAccount = pro.getProperty("senderAccount")
    val senderPassword = pro.getProperty("senderPassword")
    if(!senderAccount.isEmpty && !senderPassword.isEmpty){
      transport.connect(senderAccount,senderPassword)
    }else{
      transport.connect();
    }
    // 拼装模版消息
    val msg = new MimeMessage(session)
    msg.setFrom(new InternetAddress(pro.getProperty("senderAddress")))

    msg.setFrom(new InternetAddress(pro.getProperty("sendFrom")))
    msg.addRecipients(Message.RecipientType.TO,pro.getProperty("recipientAddress"))
    msg.setSubject(subjectType, "UTF-8")
    //设置邮件正文
    msg.setContent(messag, "text/html;charset=UTF-8")
    //设置邮件的发送时间,默认立即发送
    msg.setSentDate(new Date())
    transport.sendMessage(msg,msg.getAllRecipients)
    transport.close() //关闭连接
}

  /**
   * 发送模版消息
   * @param messag
   * @param subjectType
   * @param recivers_email
   */
  def  sendMessage(messag:String,subjectType:String,recivers_email: String): Unit ={
    println("登陆邮件服务器")
    val senderAccount = pro.getProperty("senderAccount")
    val senderPassword = pro.getProperty("senderPassword")
    if(!senderAccount.isEmpty && !senderPassword.isEmpty){
      transport.connect(senderAccount,senderPassword)
    }else{
      transport.connect();
    }
    // 拼装模版消息
    val msg = new MimeMessage(session)
    msg.setFrom(new InternetAddress(pro.getProperty("senderAddress")))

    msg.setFrom(new InternetAddress(pro.getProperty("sendFrom")))
    if(org.apache.commons.lang3.StringUtils.isNoneBlank(recivers_email)){
      msg.addRecipients(Message.RecipientType.TO,recivers_email)
    }else{
      msg.addRecipients(Message.RecipientType.TO,pro.getProperty("recipientAddress"))
    }

    msg.setSubject(subjectType, "UTF-8")
    //设置邮件正文
    msg.setContent(messag, "text/html;charset=UTF-8")
    //设置邮件的发送时间,默认立即发送
    msg.setSentDate(new Date())
    transport.sendMessage(msg,msg.getAllRecipients)
    transport.close() //关闭连接
  }


}
