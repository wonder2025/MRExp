package com.MR.Posnet;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by longhao on 2017/8/1.
 * map阶段过来的数据格式为key:imsi,timeflag value:pos,unixtime
 * 1.按unixtime从小到大进行排序
 * 2.添加OFF位的unixtime(当前时段的最后时间)
 * 3.从大到小一次相减得到每个位置的停留时间
 */
//public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

    //进行数据格式化操作
    LineFormater lineFormater = new LineFormater();
    //记录数据是pos还是net类型
    Boolean isPos;
    //要计算的日期
    String time;
    //要计算的时段
    String[] timepoint;

    /**
     * 在map阶段只调用一次,进行参数初始化的操作
     * */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //从驱动程序中获得参数并赋值
        this.time = context.getConfiguration().get("date");
        this.timepoint = context.getConfiguration().get("timepoint").split("-");

        //获得输入的文件名
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        if (fileName.startsWith("pos")) {
            this.isPos = true;
        } else if (fileName.startsWith("net")) {
            this.isPos = false;
        } else {
            throw new IOException("file name should be start with pos or net!");
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            lineFormater.format(value.toString(), this.isPos, this.time, this.timepoint);
        } catch (LineException e) {
            if (e.getFlag() == -1) {
                context.getCounter(Counter.OUTOFTIMESKIP).increment(1);
            } else if (e.getFlag() == 0) {
                context.getCounter(Counter.TIMESKIP).increment(1);
            } else {
                context.getCounter(Counter.OUTOFTIMEFLASGSKIP).increment(1);
            }
        } catch (Exception ex) {
            context.getCounter(Counter.LINESKIP).increment(1);
        }
        //输出格式[0000000000,00-09  00000174,day]    imsi,timeflag    pos,day
        context.write(lineFormater.outKey(), lineFormater.outValue());

    }
}
