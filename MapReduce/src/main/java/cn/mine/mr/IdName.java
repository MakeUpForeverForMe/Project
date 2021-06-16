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
import java.util.HashMap;
import java.util.HashSet;

public class IdName extends Configured implements Tool {
    public static class IdNameMap extends Mapper<LongWritable, Text, Text, Text> {

        private final HashMap<String, String> splitMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            Path path = new Path(context.getCacheFiles()[0].getPath());

            FSDataInputStream inputStream = FileSystem.get(context.getConfiguration()).open(path);

            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);

            BufferedReader reader = new BufferedReader(inputStreamReader);

            String line;

            while (null != (line = reader.readLine())) {
                // hash appname pcode_name code_name
                String[] splits = line.split("\t");
                if (splits.length == 1) continue;
                // hash appname
                splitMap.put(splits[0], splits[1]);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");

            String string = splitMap.get(splits[1]);

            if (null == string || "".equals(string)) string = splits[2];

            if (null != string && (!"".equals(string))) context.write(new Text(splits[0]), new Text(string));
        }
    }

    public static class IdNameReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<Text> hashSet = new HashSet<>();

            // 为了去重，使用HashSet
            for (Text value : values) {
                hashSet.add(value);
            }

            // 将去重后的数据写出
            for (Text value : hashSet) {
                context.write(key, value);
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());

        job.setJarByClass(this.getClass());

        job.addCacheFile(URI.create(strings[0]));

        FileInputFormat.addInputPath(job, new Path(strings[1]));

        job.setMapperClass(IdNameMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(IdNameReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path path = new Path(strings[2]);

        FileSystem.get(this.getConf()).delete(path, true);

        FileOutputFormat.setOutputPath(job, path);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
