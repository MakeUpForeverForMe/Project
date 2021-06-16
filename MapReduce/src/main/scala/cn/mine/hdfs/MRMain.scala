package cn.mine.hdfs

import cn.mine.mr.{DailyNum, IdName, RepeatRate}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ToolRunner

/**
  * @author ximing.wei
  */
object MRMain {
    def main(args: Array[String]): Unit = {
        val configuration = new Configuration(ConfProperty("conf.properties").conf)
        // configuration.set("mapred.task.timeout", "0");
        // configuration.set("mapred.textoutputformat.ignoreseparator", "true");
        // configuration.set("mapred.textoutputformat.separator", "=");

        val name_hash   = "name_hash.tsv"
        val tdid_hash   = "tdid_hash"
        val tdid_name   = "tdid_name"
        val daily_num   = "daily_num"
        val repeat_rate = "repeat_rate"

        if (ToolRunner.run(configuration, new IdName, Array[String](name_hash, tdid_hash, tdid_name)) == 0)
            System.out.println("获取原始数据  成功")
        else {
            System.out.println("获取原始数据  失败")
            System.exit(1)
        }

        if (ToolRunner.run(configuration, new DailyNum, Array[String](tdid_name, daily_num)) == 0)
            System.out.println("计算日活  成功")
        else {
            System.out.println("计算日活  失败")
            System.exit(1)
        }

        if (ToolRunner.run(configuration, new RepeatRate, Array[String](daily_num, tdid_name, repeat_rate)) == 0)
            System.out.println("计算App重复率  成功")
        else {
            System.out.println("计算App重复率  失败")
            System.exit(1)
        }
    }
}
