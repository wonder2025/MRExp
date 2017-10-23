package com.MR.sql;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GroupBy extends Configured implements Tool {
//    hdfs:输入文件的路"/user/longhao/statistical_input/orders.txt"
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new GroupBy(), args);
    }

    public int run(String[] strings) throws Exception {
        return 0;
    }
}
