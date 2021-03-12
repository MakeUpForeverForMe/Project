package com.weshare.util;

import com.weshare.core.AssetFileToHive;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mouzwang on 2020-11-30 18:10
 */
public class FileCheckUtils {
    /**
     * check first arg : basic file name
     * @param fileName
     */
    public static void checkFileName(String fileName) {
        if (fileName == null) {
            System.out.println("the file name is null");
            return;
        }
        if (!fileName.equals("project_info") && ! fileName.equals("project_due_bill_no") && !fileName.equals("bag_info")
                 && !fileName.equals("bag_due_bill_no")){
            System.out.println("an illegal file name!");
            return;
        }
    }

    /**
     * check second arg : project id or bag id
     * @param fileId
     */
    public static void checkFileId(String fileId) {
        if (StringUtils.isBlank(fileId)) {
            System.out.println("the project id or bag id is not correct!");
            return;
        }
    }

}
