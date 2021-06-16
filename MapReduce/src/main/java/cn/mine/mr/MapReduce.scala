package cn.mine.mr

import java.lang

import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.Tool
import org.apache.log4j.{Level, Logger}

import scala.collection.JavaConversions._


object MapReduce extends Configured with Tool {

    private class MRTaskMap extends Mapper[LongWritable, Text, Text, IntWritable] {
        override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
            val strings = value.toString.split(',')
            context.write(new Text(strings(3)), new IntWritable(1))
        }
    }

    private class MRTaskReduce extends Reducer[Text, IntWritable, Text, IntWritable] {
        override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
            var sum = 0
            for (value <- values) sum += value.get()
            context.write(key, new IntWritable(sum))
        }
    }


    private def path(path: String) = new Path(path)

    override def run(args: Array[String]): Int = {
        val job = Job.getInstance(getConf)

        FileInputFormat.setInputPaths(job, path(args(0)))
        job.setMapperClass(classOf[MRTaskMap])
        job.setMapOutputKeyClass(classOf[Text])
        job.setMapOutputValueClass(classOf[IntWritable])

        FileSystem.get(getConf).delete(path(args(1)), true)

        job.setReducerClass(classOf[MRTaskReduce])
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[IntWritable])
        FileOutputFormat.setOutputPath(job, path(args(1)))

        if (job.waitForCompletion(true)) 0 else 1
    }


    Logger.getRootLogger.setLevel(Level.INFO)
    Logger.getRootLogger.fatal("RootLogger Level : " + Logger.getRootLogger.getLevel)
    private val logger: Logger = Logger.getLogger(this.getClass)

    import logger._

    def main(args: Array[String]): Unit = {
        Logger.getRootLogger.setLevel(Level.ERROR)
        fatal("RootLogger Level : " + logger.getParent.getLevel)

        val inputFile_HDFS = "/tmp/dm_user_info_tmp/dm_user_info.csv"
        val outputDir_HDFS = "/tmp/dm_user_info_sum"

        val inputFile = "D:\\Users\\ximing.wei\\Desktop\\cc\\dm_user_info.csv"
        val outputDir = "D:\\Users\\ximing.wei\\Desktop\\aa"


        setConf(new Configuration())
        val array = Array(inputFile, outputDir)

        val i = run(array)
        fatal(if (i == 0) "结果 : MR执行成功" else "结果 : MR执行失败")
    }
}
