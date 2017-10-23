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
 * Created by longhao on 2017/8/1.
 * Email: longhao1@email.szu.edu.cn
 */
public class Posnet extends Configured implements Tool {

    public  int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("");
            System.err.println("Usage: Posnet <date> <timepoint> <isTotal>");
            System.err.println("Example: Posnet 2016-02-21 09-18-24 1");
            System.exit(-1);
        }
        Configuration conf = getConf();
        conf.set("date", args[1]);
        conf.set("timepoint", args[2]);
        conf.set("mapreduce.job.jar", "./target/MRExp-1.0-SNAPSHOT.jar");
//        conf.set("mapreduce.job.jar", "./out/artifacts/MRExp_jar/MRExp.jar");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        //使用统计最长停留时间的reducer
        Job job = Job.getInstance(conf,"posnet");
        job.setJarByClass(Posnet.class);
        Path job_output = new Path("/user/scdx03/out");
        Path job_input = new Path("/user/scdx03/input/pos.txt");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Mapper.class);
        if (args[3].equals("1")) {
            job.setReducerClass(TotalReducer.class);
        }
        else{
            job.setReducerClass(Reducer.class);
        }
        job.setNumReduceTasks(1);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, job_input);
        job_output.getFileSystem(conf).delete(job_output, true);
        FileOutputFormat.setOutputPath(job, job_output);
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception{

        ToolRunner.run(new Posnet(),args);
    }
}