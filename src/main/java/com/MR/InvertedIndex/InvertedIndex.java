package com.MR.InvertedIndex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

public class InvertedIndex extends Configured implements Tool {
    public static class InvertedIndexMapper extends Mapper<LongWritable,Text,Text,Text> {
        private Text k=new Text();
        private Text v=new Text();
        private FileSplit fileSplit;

        protected void map( LongWritable key, Text value,Context context) throws IOException,InterruptedException{
            fileSplit=(FileSplit)context.getInputSplit();
            StringTokenizer stk = new StringTokenizer(value.toString());
                while (stk.hasMoreElements()) {
                    //单词加上读取文件的路径
                k.set(stk.nextToken() + ">" + fileSplit.getPath().toString());
                v.set("1");
                context.write(k, v);
            }
        }
    }
//每个文件中的处理Combiner，仅仅局限在mapper所在的节点上
    public static class InvertedIndexCombiner extends Reducer<Text,Text,Text,Text> {
        private Text v=new Text();

        protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
            int counts = 0;
            for (Text t : values) {
                counts += Integer.parseInt(t.toString());
            }
            String[] names = key.toString().split(">");
            key.set(names[0]);
            v.set(names[1] + ":" + counts);
            context.write(key,v);
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text,Text,Text,Text>{
        Text value=new Text();
        //key=单词 value=[{文件路径1：在该文件中出现的次数1}{文件路径2：在该文件中出现的次数2}...]
        protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
            String str = new String();
            for (Text t : values) {
                //将value做连接
                str+=t.toString()+";"+"\n";
            }
            value.set(str);
            context.write(key,value);
        }
    }

    public int run(String[] args) throws Exception{
        Configuration conf = getConf();
        conf.set("mapreduce.job.jar", "./target/MRExp-1.0-SNAPSHOT.jar");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job = Job.getInstance(conf,"scdx03_InvertedIndex");

        job.setJarByClass(InvertedIndex.class);
        Path job_output = new Path("/user/scdx03/out");
        Path job_input = new Path("/user/scdx03/invertedindex_input");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(InvertedIndexMapper.class);
        //Combiner是本地的处理
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
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
        System.setProperty("hadoop.home.dir", "E:\\hadoop");
        ToolRunner.run(new InvertedIndex(),args);
    }
}
