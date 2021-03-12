package com.weshare.bigdata.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

/**
 * @author yuheng.wang
 * @date 2020/10/21 15:47
 * @Description
 */
public class SendEmailUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(SendEmailUtil.class);
    private static InputStream inputStream = null;
    private static Properties prop = null;

    public static Session initSession() throws NoSuchProviderException, IOException {
        inputStream = ClassLoader.getSystemResourceAsStream("email.properties");
        prop = new Properties();
        prop.load(inputStream);
        inputStream.close();
        return Session.getInstance(prop);
    }

    public static void sendEmail(String subjectInfo,String content){
        Transport transport = null;
        try {
            //open Transport
            Session session = initSession();
            String senderAccount = prop.getProperty("senderAccount");
            String senderPassword = prop.getProperty("senderPassword");
            LOGGER.warn("senderAccount name is : " + senderAccount);
            transport = session.getTransport();
            //create transport connection
            if (!StringUtils.isBlank(senderAccount) && !StringUtils.isBlank(senderPassword)) {
                transport.connect(senderAccount,senderPassword);
                LOGGER.warn("transport connect success!");
            } else {
                transport.connect();
                LOGGER.warn("transport connect without account name or password!");
            }
            //create MimeMessage
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(prop.getProperty("sendFrom",prop.getProperty("senderAddress"))));
            message.setRecipients(Message.RecipientType.TO, prop.getProperty("recipientAddress"));
            message.setSubject(subjectInfo,"UTF-8");
            message.setContent(content, "text/html;charset=UTF-8");
            //new Date() means send email immediately
            message.setSentDate(new Date());
            transport.sendMessage(message,message.getAllRecipients());
            transport.close();
        } catch (Exception e) {
            LOGGER.error("send email failed.",e);
            LOGGER.error("email title:" + subjectInfo + ", email content:" + content);
        } finally{
            try {
                transport.close();
            } catch (MessagingException e) {
                e.printStackTrace();
            }
        }
    }




}
