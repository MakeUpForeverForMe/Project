package com.weshare.bigdata.wind;

import com.alibaba.fastjson.JSONObject;
import com.weshare.bigdata.util.AESPlus;
import com.weshare.bigdata.util.HTTPUtils;
import com.weshare.bigdata.util.MsgUtils;
import com.weshare.bigdata.util.SendEmailUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

/**
 * created by chao.guo on 2021/5/25
 **/
public class WindController {
    private static final Logger logger = LoggerFactory.getLogger(WindController.class);

    public static void main(String[] args) throws Exception {
        if(null==args || args.length<4){
            logger.error(" 密匙key ，url， filePath， file_name is not alowed empty!");
            return ;
        }
        String key=args[0];
        String url = args[1];
        String filePath = args[2];
        String file_name = args[3];
        logger.info("url:{},filePath:{},file_name:{}",url,filePath,file_name);


        String applyNo = UUID.randomUUID().toString();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        String timeStamp = simpleDateFormat.format(new Date());
        HashMap<String, String> requestParam = new HashMap<>();
        requestParam.put("applyNo",applyNo);
        requestParam.put("timeStamp",timeStamp);


        HashMap<String, String> reqdData = new HashMap<>();
        reqdData.put("filePath",filePath);reqdData.put("fileName",file_name);

        String reqDataEncoder = AESPlus.getStr(AESPlus.encrypt(key, JSONObject.toJSONString(reqdData)));
        requestParam.put("reqData",reqDataEncoder);
        String request_body = JSONObject.toJSONString(requestParam);
        String result = HTTPUtils.sendPost(url, request_body);
        logger.info("result:{}",result);
        String error_msg="调用风控接口成功！";
        JSONObject resultJson = JSONObject.parseObject(result);
        if(!StringUtils.equals("0",resultJson.getString("retCode"))){ // 不成功则
            error_msg=result;
        }
        HashMap<Object, Object> mesMap = new HashMap<>();
        String[] s = file_name.split("_");
        mesMap.put("project_id",s[4]);
        mesMap.put("biz_date",s[5].split("\\.")[0]);
        mesMap.put("message",error_msg);
        Properties properties = new Properties();
        SendEmailUtil.initSession();
        properties.load(WindController.class.getClassLoader().getResourceAsStream("emr_properties.properties"));;
        SendEmailUtil.sendEmail("数据中台-风控交互通知",JSONObject.toJSONString(mesMap));

    }


}
