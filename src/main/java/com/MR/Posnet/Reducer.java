package com.MR.Posnet;

import com.MR.Posnet.Util;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
        * Created by longhao on 2017/8/1.
        *统计每个用户在不同时段中各个基站的停留时间
        */
public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, NullWritable, Text> {

    //要计算的时间
    String day;
    Text out = new Text();

    /**
     * reduce阶段只执行一次,用于参数初始化等
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.day = context.getConfiguration().get("date");
    }
    //[0000000000,00-09 {  (00000174,2016-02-21 00:33:28)(...)(...).....}][imsi,timeflag (pos,day)(...)...]
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //使用TreeMap存储,key为unixtime,自动排序
        TreeMap<Long, String> sortedData = Util.getSortedData(context, values);
        String[] ks = key.toString().split(",");
        String imsi = ks[0];
        String timeflag = ks[1];
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            //设置该数据所在的最后时段的unixtime
            Date offTimeflag = simpleDateFormat.parse(this.day + " " + timeflag.split("-")[1] + ":00:00");

            sortedData.put(offTimeflag.getTime() / 1000L, "OFF");
            //计算两两之间的时间间隔
            HashMap<String, Float> resMap = Util.calcStayTime(sortedData);
            //循环输出
            for (Map.Entry<String, Float> entry : resMap.entrySet()) {
                String builder = imsi + "|" +
                        timeflag + "|" +
                        entry.getKey() + "|" +
                        entry.getValue();
                out.set(builder);
                context.write(NullWritable.get(), out);
            }
        } catch (ParseException e) {
            context.getCounter(Counter.TIMEFORMATERR).increment(1);
        }
    }
}