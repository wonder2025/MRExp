package com.MR.Posnet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 只保留每个用户每个时间段停留时间最长的基站位置0001 09-18 00003 15
 * 输出格式key=imsi,timeflag value= pos,day 形如[0000000000,00-09  00000174,day]
 */
public class TotalReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, NullWritable, Text> {
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

    //传入reduce的key=imsi+时段，value=｛(基站位置1，时间点1)，（基站位置2，时间点2）....｝
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
            //计算两两之间的时间间隔 ，sortedData是一个装着时间点和位置信息的treemap，经过calcStayTime计算后变成了装着基站位置和停留时间的hashmap
            HashMap<String, Float> resMap = Util.calcStayTime(sortedData);
            //--------------------------以上部分与Reducer类的逻辑一样--------------------------------------
            //建立临时变量
            Float maxTime = 0f;
            String maxPos = null;
            //找出在同一用户在同一时段的停留最大时间所对应的基站
            for (Map.Entry<String, Float> entry : resMap.entrySet()) {
                if (maxTime < entry.getValue()) {
                    maxTime = entry.getValue();
                    maxPos = entry.getKey();
                }
            }
            String builder = imsi + "|" +
                    timeflag + "|" +
                    maxPos + "|" +
                    maxTime;
            out.set(builder);
            //key设为空，仅仅输出value部分
            context.write(NullWritable.get(), out);
        } catch (ParseException e) {
            context.getCounter(Counter.TIMEFORMATERR).increment(1);
        }
    }
}
