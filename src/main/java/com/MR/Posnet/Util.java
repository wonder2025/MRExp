package com.MR.Posnet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * 通用工具类
 */
public class Util {

    /**
     * 分隔符类型,使用正则表达式,表示分隔符为\t或者,
     * 使用方法为SPARATOR.split(字符串)
     */
    public static final Pattern SPARATOR = Pattern.compile("[\t,]");

    /**
     * HDFS路径的根目录
     */
    public static final String HDFS = "hdfs://hdfs-production";


    /**
     * 计算unixtime两两之间的时间差
     *
     * @param sortDatas key为unixtime,value为pos
     * @return key为pos, value为该pos的停留时间
     */
    public static HashMap<String, Float> calcStayTime(TreeMap<Long, String> sortDatas) {
        //  sortDatas是的treemap放入形如 2016-02-21 00:33:28，00000174的数据
        HashMap<String, Float> resMap = new HashMap<String, Float>();

        Iterator<Long> iter = sortDatas.keySet().iterator();
        Long currentTimeflag = iter.next();
        //遍历treemap 每次在某个基站停留的时间
        while (iter.hasNext()) {
            Long nextTimeflag = iter.next();
            float diff = (nextTimeflag - currentTimeflag) / 60.0f;
            //超过60分钟过滤不计
            if (diff <= 60.0) {
                String currentPos = sortDatas.get(currentTimeflag);
                if (resMap.containsKey(currentPos)) {
                    //如果resMap中已经存在pos则直接叠加时间
                    resMap.put(currentPos, resMap.get(currentPos) + diff);
                } else {
                    resMap.put(currentPos, diff);
                }
            }
            currentTimeflag = nextTimeflag;
        }
        return resMap;
    }

    /**
     * 将map阶段传递过来的数据按照unixtime从小到大排序(使用TreeMap)
     *
     * @param context reducer的context上下文,用于设置counter
     * @param values  map阶段传递过来的数据
     * @return key为unixtime, value为pos
     */
    public static TreeMap<Long, String> getSortedData(Reducer.Context context, Iterable<Text> values) {
        //[0000000000,00-09 {  (00000174,2016-02-21 00:33:28)(...)(...).....}][imsi,timeflag (pos,day)(...)...]
        TreeMap<Long, String> sortedData = new TreeMap<Long, String>();
        for (Text v : values) {
            String[] vs = v.toString().split(",");
            try {
                //00000174,2016-02-21 00:33:28
                //根据时间进行排序
                sortedData.put(Long.parseLong(vs[1]), vs[0]);
            } catch (NumberFormatException num) {
                context.getCounter(Counter.TIMESKIP).increment(1);
            }
        }
        return sortedData;
    }
}