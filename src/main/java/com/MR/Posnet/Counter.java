package com.MR.Posnet;
//枚举类型，列举异常情况类型
public enum Counter {
    TIMESKIP,     //时间格式有误
    OUTOFTIMEFLASGSKIP,    //时间超出最大时段
    LINESKIP,     //源文件行有误
    OUTOFTIMESKIP,   //不在当前时间内
    TIMEFORMATERR,   //时间格式化错误
}
