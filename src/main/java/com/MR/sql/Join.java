package com.MR.sql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class Join extends Configured implements Tool {

    /**
     * 分隔符类型,使用正则表达式,表示分隔符为\t或者,
     * 使用方法为SPARATOR.split(字符串)
     */
    public static final Pattern SPARATOR = Pattern.compile("[\t,]");

    public static class JoinMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        IntWritable k = new IntWritable();
        Text v = new Text();

        /**
         * 根据map-reduce过程中分组和shuffle的特性,相同key的value最终会到一个reducer中处理
         * 所以只需要将join的关键字段作为key
         * value则是标识位+内容的格式
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            String[] tokens = SPARATOR.split(value.toString());
            if (fileName.contains("customer")) {
                k.set(Integer.parseInt(tokens[0]));
                //value中的key为0表示值是customer表的
                v.set(new Text("0-" + tokens[1]));
            } else {
                k.set(Integer.parseInt(tokens[0]));
                //value中的key为1表示值是orders表的
                v.set("1-" + new Text(tokens[1]));
            }
            context.write(k, v);
        }
    }

    public static class JoinReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        Text k = new Text();
        IntWritable v = new IntWritable();

        /**
         * 处理的时候要注意是left join还是right join,本示例为left join
         */
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String customerName = "";
            Set<Integer> orders = new HashSet<Integer>();
            //提取customer和order表的信息
            //这里可以使用更为简便的方式,如map时使用多类型的value来区分,这里重点在于map和reduce中间连接的key部分
            for (Text value : values) {
                if (value.toString().startsWith("0")) {
                    customerName = value.toString().split("-")[1];
                }
                if (value.toString().startsWith("1")) {
                    orders.add(Integer.parseInt(value.toString().split("-")[1]));
                }
            }
            k.set(customerName);
            //left join 以左表customer为基础
            for (Integer order : orders) {
                v.set(order);
                context.write(k, v);
            }
        }
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.job.jar", "out/artifacts/MapReduceDemo_jar/MapReduceDemo.jar");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job = Job.getInstance(conf, "sql_join");

        job.setJarByClass(Join.class);
        Path job_output = new Path("/user/longhao/out");

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("/user/longhao/statistical_input"));
        job_output.getFileSystem(conf).delete(job_output, true);
        FileOutputFormat.setOutputPath(job, job_output);

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Join(), args);
    }

}
