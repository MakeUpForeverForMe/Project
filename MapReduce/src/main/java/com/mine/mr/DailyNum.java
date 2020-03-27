package com.mine.mr;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Iterator;

public class DailyNum extends Configured implements Tool {
  public class DailyNumMap extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // 获取 id_name 表，tdid appName
      String[] splits = value.toString().split("\t");
      context.write(new Text(splits[1]), new Text(splits[0]));
    }
  }

  public class DailyNumReduce extends Reducer<Text, Text, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      // appName tdid(数组)
      int sum = 0;
      Iterator<Text> iterator = values.iterator();
      // 根据 appName 计算 tdid 数量，即App的日活
      while (iterator.hasNext()) {
        sum += 1;
        iterator.next();
      }
      // appName sum
      context.write(key, new LongWritable(sum));
    }
  }

  @Override
  public int run(String[] strings) throws Exception {
    Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());

    job.setJarByClass(this.getClass());

    FileInputFormat.addInputPath(job, new Path(strings[0]));

    job.setMapperClass(DailyNumMap.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setNumReduceTasks(1);

    job.setReducerClass(DailyNumReduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    Path path = new Path(strings[1]);

    FileSystem.get(this.getConf()).delete(path, true);

    FileOutputFormat.setOutputPath(job, path);

    return job.waitForCompletion(true) ? 0 : 1;
  }
}
