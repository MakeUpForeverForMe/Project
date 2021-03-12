package com.weshare.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
/**
 * Created by mouzwang on 2020-11-30 16:00
 */
public class HadoopUtils {
    public static void upLoadFiles(String targetPath,String fileLocalPath) throws IOException {
        Configuration conf = new Configuration();
        conf.set("dfs.replication","2");
        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(new Path(fileLocalPath), new Path(targetPath));
    }
}
