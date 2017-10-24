package com.MR.sql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.regex.Pattern;

public class Statistical extends Configured implements Tool {

    public static class StatisticalMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text k = new Text("key");
        IntWritable v = new IntWritable();

        /**
         * 分隔符类型,使用正则表达式,表示分隔符为\t或者,
         * 使用方法为SPARATOR.split(字符串)
         */
        public static final Pattern SPARATOR = Pattern.compile("[\t,]");

        /**
         * 数据格式为name    age
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = SPARATOR.split(value.toString());
            v.set(Integer.parseInt(tokens[tokens.length-1]));
            context.write(k, v);
        }
    }

    public static class StatisticalReducer extends Reducer<Text, IntWritable, NullWritable, Text> {
        Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int max = 0;
            int min = Integer.MAX_VALUE;
            int sum = 0;
            int count = 0;
            for (IntWritable value : values) {
                if (max < value.get()) {
                    max = value.get();
                }
                if (min > value.get()) {
                    min = value.get();
                }
                sum += value.get();
                count++;
            }
            v.set(new Text("avg:"+Integer.toString(sum / count)+",max:"+Integer.toString(max)+",min:"+Integer.toString(min)));
            context.write(NullWritable.get(), v);
        }
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.job.jar", "./target/MRExp-1.0-SNAPSHOT.jar");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job = Job.getInstance(conf, "sql_statistical");

        job.setJarByClass(Statistical.class);
        Path job_output = new Path("/user/scdx03/out");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(StatisticalMapper.class);
        job.setReducerClass(StatisticalReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("/user/scdx03/statistical_input/customer.txt"));
        job_output.getFileSystem(conf).delete(job_output, true);
        FileOutputFormat.setOutputPath(job, job_output);
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Statistical(), args);
    }

}
