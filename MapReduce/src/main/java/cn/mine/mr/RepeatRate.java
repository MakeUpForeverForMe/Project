package cn.mine.mr;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

public class RepeatRate extends Configured implements Tool {
    public static class RepeatRateMap extends Mapper<LongWritable, Text, Text, Text> {

        private final HashMap<String, String> splitMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            Path path = new Path(context.getCacheFiles()[0]);

            FSDataInputStream inputStream = FileSystem.get(context.getConfiguration()).open(path);

            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);

            BufferedReader reader = new BufferedReader(inputStreamReader);

            String line;

            while ((line = reader.readLine()) != null) {

                String[] splits = line.split("\t");
                // appName sum
                splitMap.put(splits[0], splits[1]);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // tdid appName
            String[] splits = value.toString().split("\t");
            // 根据appName返回sum
            String string = splitMap.get(splits[1]);
            // 得到appName=sum
            if (null != string && (!"".equals(string))) string = splits[1] + "=" + string;
            else return;
            // 传出key : tdid  value : appName=sum_{a,b}
            context.write(new Text(splits[0]), new Text(string + "_a"));
            context.write(new Text(splits[0]), new Text(string + "_b"));
        }
    }

    public static class RepeatRateReduce extends Reducer<Text, Text, Text, Text> {
        // 用于存储 App1-App2 sum(重复人数)
        HashMap<String, Long> hashMap = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) {
            // 用于存储 App1
            ArrayList<String> arrayListA = new ArrayList<>();
            // 用于存储 App2
            ArrayList<String> arrayListB = new ArrayList<>();
            // appName=sum_{a,b}
            for (Text value : values) {
                // appName=sum  {a,b}
                String[] split = value.toString().split("_");
                if ("a".equals(split[1])) arrayListA.add(split[0]);
                else if ("b".equals(split[1])) arrayListB.add(split[0]);
            }

            for (String stringA : arrayListA) {
                for (String stringB : arrayListB) {
                    String app = stringA + "\t" + stringB;

                    Long sum = hashMap.get(app);
                    if (sum == null) sum = 0L;
                    sum += 1;
                    hashMap.put(app, sum);
                }
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());

        job.setJarByClass(this.getClass());

        job.addCacheFile(URI.create(strings[0]));

        FileInputFormat.addInputPath(job, new Path(strings[1]));

        job.setMapperClass(RepeatRateMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(RepeatRateReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        Path path = new Path(strings[2]);

        FileSystem.get(this.getConf()).delete(path, true);

        FileOutputFormat.setOutputPath(job, path);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
