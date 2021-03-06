package com.MR.Posnet;

import com.MR.Posnet.TotalReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;


import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 程序的主类
 *
 */
public class Posnet extends Configured implements Tool {
    //args为用户传入的参数数组
    public  int run(String[] args) throws Exception {
        //检查用户传入的参数数组是否齐全
        if (args.length < 3) {
            System.err.println("");
            System.err.println("Usage: Posnet <date> <timepoint> <isTotal>");
            System.err.println("Example: Posnet 2016-02-21 09-18-24 1");
            System.exit(-1);
        }
        Configuration conf = getConf();
        conf.set("date", args[1]);
        conf.set("timepoint", args[2]);
        //jar包所在路径
        conf.set("mapreduce.job.jar", "./target/MRExp-1.0-SNAPSHOT.jar");
        //跨平台
        conf.set("mapreduce.app-submission.cross-platform", "true");
        //使用统计最长停留时间的reducer
        Job job = Job.getInstance(conf,"posnet");
        job.setJarByClass(Posnet.class);
        Path job_output = new Path("/user/scdx03/out");
        Path job_input = new Path("/user/scdx03/input/pos.txt");
        //reduce输出key类型
        job.setOutputKeyClass(Text.class);
        //reduce输出value类型
        job.setOutputValueClass(Text.class);
        //设置map类
        job.setMapperClass(Mapper.class);
        if (args[3].equals("1")) {
            job.setReducerClass(TotalReducer.class);
        }
        else{
            job.setReducerClass(Reducer.class);
        }
        //设置reduce的个数
        job.setNumReduceTasks(1);
        //输入文件类型
        job.setInputFormatClass(TextInputFormat.class);
        //输出文件类型
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输入文件路径
        FileInputFormat.setInputPaths(job, job_input);
        //如果存在输出结果则先删除
        job_output.getFileSystem(conf).delete(job_output, true);
        //设置输出路径
        FileOutputFormat.setOutputPath(job, job_output);
        //等待mapreduce执行完
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception{

        ToolRunner.run(new Posnet(),args);
    }
}