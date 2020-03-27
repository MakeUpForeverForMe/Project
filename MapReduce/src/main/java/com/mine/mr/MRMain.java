package com.mine.mr;

import com.mine.utils.ConfProperty;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class MRMain {
  public static void main(String[] args) throws Exception {
    Configuration configuration = new Configuration(new ConfProperty("conf.properties").conf());
    // configuration.set("mapred.task.timeout", "0");
    // configuration.set("mapred.textoutputformat.ignoreseparator", "true");
    // configuration.set("mapred.textoutputformat.separator", "=");

    String name_hash = "name_hash.tsv";
    String tdid_hash = "tdid_hash";
    String tdid_name = "tdid_name";
    String daily_num = "daily_num";
    String repeat_rate = "repeat_rate";

    int idName = ToolRunner.run(configuration, new IdName(), new String[]{name_hash, tdid_hash, tdid_name});

    if (idName == 0) {
      System.out.println("获取原始数据  成功");
    } else {
      System.out.println("获取原始数据  失败");
      System.exit(idName);
    }

    int dailyNum = ToolRunner.run(configuration, new DailyNum(), new String[]{tdid_name, daily_num});

    if (dailyNum == 0) {
      System.out.println("计算日活  成功");
    } else {
      System.out.println("计算日活  失败");
      System.exit(dailyNum);
    }

    int repeatRate = ToolRunner.run(configuration, new RepeatRate(), new String[]{daily_num, tdid_name, repeat_rate});

    if (repeatRate == 0) {
      System.out.println("计算App重复率  成功");
    } else {
      System.out.println("计算App重复率  失败");
      System.exit(repeatRate);
    }

  }
}
