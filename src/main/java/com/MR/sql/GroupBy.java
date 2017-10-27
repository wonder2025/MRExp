package com.MR.sql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class GroupBy extends Configured implements Tool {
    public static final Pattern SPARATOR = Pattern.compile("[,]");

    //自定义writer接口
    protected static class IntPair implements WritableComparable<IntPair> {
        private int first = 0;
        private int second = 0;

        public void set(int left, int right) {
            first = left;
            second = right;
        }

        public int getFirst() {
            return first;
        }

        public int getSecond() {
            return second;
        }

        //反序列化，从流中的二进制转换成IntPair
        public void readFields(DataInput in) throws IOException {
            first = in.readInt();
            second = in.readInt();
        }

        //序列化，将IntPair转化成使用流传送的二进制
        public void write(DataOutput out) throws IOException {
            out.writeInt(first);
            out.writeInt(second);
        }

        @Override
        public int hashCode() {
            return first + "".hashCode() + second + "".hashCode();
        }

        @Override
        public boolean equals(Object right) {
            if (right instanceof IntPair) {
                IntPair r = (IntPair) right;
                return r.first == first && r.second == second;
            } else {
                return false;
            }
        }

        //对key排序时，调用的就是这个compareTo方法
        public int compareTo(IntPair o) {
            if (first != o.first) {
                return first - o.first;
            } else if (second != o.second) {
                return second - o.second;
            } else {
                return 0;
            }
        }

    }

    public static class GroupByMapper extends Mapper<LongWritable, Text, IntPair, IntWritable> {

        private final IntPair key = new IntPair();
        private final IntWritable value = new IntWritable();

        @Override
        public void map(LongWritable inKey, Text inValue, Context context) throws IOException, InterruptedException {
            //切分一行数据
            String[] tokens = SPARATOR.split(inValue.toString());
            //得到第一个数
            int first = Integer.parseInt(tokens[0]);
            //得到第二个数
            int second = Integer.parseInt(tokens[1]);

            key.set(first, second);
            value.set(second);
            context.write(key, value);
//            在之后的partition阶段对key进行排序，调用IntPair的compareTo方法
        }
    }

    //每个文件中的处理Combiner，仅仅局限在mapper所在的节点上
    public static class GroupByCombiner extends Reducer<IntPair, IntWritable, IntPair, IntWritable> {
        private IntWritable v = new IntWritable();

        //计算每个key的个数，
        protected void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int counts = 0;
            for (IntWritable t : values) {
                counts += 1;
            }
            v.set(counts);
            context.write(key, v);
        }
    }

    public static class GroupByReducer extends Reducer<IntPair, IntWritable, NullWritable, Text> {
        Text out = new Text();

        @Override
        public void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int counts = 0;
            //计算key的总和
            for (IntWritable t : values) {
                counts += Integer.parseInt(t.toString());
            }
            String builder = key.getFirst() + "|" + key.getSecond() + "|" + counts;
            out.set(builder);
            context.write(NullWritable.get(), out);
        }
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.job.jar", "./target/MRExp-1.0-SNAPSHOT.jar");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job = Job.getInstance(conf, "sql_groupby");

        job.setJarByClass(GroupBy.class);
        Path job_output = new Path("/user/scdx03/out");

        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置reduce的输出key的类型
        job.setOutputKeyClass(Text.class);
        //设置reduce的输出的类型
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(GroupByMapper.class);
        job.setCombinerClass(GroupByCombiner.class);
        job.setReducerClass(GroupByReducer.class);
        //只有一个，此时输出的key有序
        job.setNumReduceTasks(1);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("/user/scdx03/statistical_input/orders.txt"));
        job_output.getFileSystem(conf).delete(job_output, true);
        FileOutputFormat.setOutputPath(job, job_output);

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new GroupBy(), args);
    }
}
