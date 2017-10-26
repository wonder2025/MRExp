package com.MR.test;


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
import java.util.Iterator;

/**
 * 测试本身mapreduce存在排序
 */
public class Test extends Configured implements Tool {
    public static class WordCountMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {


        private IntWritable mapOutputKey = new IntWritable();
// private final static IntWritable mapOutputValue = new IntWritable(1);


        @Override
        public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
// parse to int
            String lineValue = value.toString();
            Integer integer = Integer.valueOf(lineValue);
// set value and output
            mapOutputKey.set(integer);
            context.write(mapOutputKey, mapOutputKey);


        }
    }
    // 2: reduce class


    public static class WordCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {


        private IntWritable outPutValue = new IntWritable();


        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> valuess, Context context)
                throws IOException, InterruptedException {
            Iterator<IntWritable> iterator = valuess.iterator();
            while (iterator.hasNext()) {
                IntWritable value = (IntWritable) iterator.next();
// output
                context.write(key, value);
            }
        }
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.job.jar", "./target/MRExp-1.0-SNAPSHOT.jar");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job = Job.getInstance(conf, "sql_test");

        job.setJarByClass(Test.class);
        Path job_output = new Path("/user/scdx03/out");

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job, new Path("/user/scdx03/statistical_input/sort.txt"));
        job_output.getFileSystem(conf).delete(job_output, true);
        FileOutputFormat.setOutputPath(job, job_output);
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Test(), args);
    }
}
//        1       1
//        2       2
//        789     789
//        10      10
//        11      11
//        12      12
//        12      12
//        12      12
//        12      12
//        17      17
//        20      20
//        21      21
//        32      32
//        34      34
//        34      34
//        34      34
//        35      35
//        38      38
//        45      45
//        45      45
//        45      45
//        53      53
//        56      56
//        57      57
//        57      57
//        456     456
//        72      72
//        78      78
//        89      89
//        90      90
//        92      92
//        93      93
//        93      93
//        11111   11111
//        543     543
//        654     654
//        678     678
//        345     345

//用1个reduce
//        1       1
//        2       2
//        10      10
//        11      11
//        12      12
//        12      12
//        12      12
//        12      12
//        17      17
//        20      20
//        21      21
//        32      32
//        34      34
//        34      34
//        34      34
//        35      35
//        38      38
//        45      45
//        45      45
//        45      45
//        53      53
//        56      56
//        57      57
//        57      57
//        72      72
//        78      78
//        89      89
//        90      90
//        92      92
//        93      93
//        93      93
//        345     345
//        456     456
//        543     543
//        654     654
//        678     678
//        789     789
//        11111   11111