package com.hive;
    import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import java.lang.String;

@Description(
        name = "getweight",
        value = "_FUNC_(consumerid, city_no, industrytype) - from the input, " +
                "returns the weight of the consumer.",
        extended = "Example:\n" +
                " > SELECT _FUNC_(consumerid, city_no, industrytype) FROM src;\n")

public class GetWeight extends UDF {

    public GetWeight() {
    }

    public String evaluate(String consumerid, String city_no, String industrytype) {
        //将CITY_NO和INDUSTRYTYPE的权重相加即作为该企业的供电等级的评判依据
        int score = this.city_score(city_no) + this.industrytype_score(industrytype);
        //返回consumerid和权重score
        return consumerid + "\t" + Integer.toString(score);
    }
    //根据城市no，返回供电权重
    public int city_score(String city_no) {
        // before java7, switch didn't support the type of string
        String char1 = city_no.substring(0, 1) ;
        int a = Integer.parseInt(char1);
        String char2 = city_no.substring(1, 2) ;
        int b = Integer.parseInt(char2);

        switch ( a * 10 + b ) {
            case 1: return 1;
            case 2: return 2;
            case 3: return 3;
            case 4: return 4;
            case 5: return 5;
            case 6: return 6;
            case 7: return 7;
            case 8: return 8;
            case 9: return 9;
            case 12: return 10;
            case 13: return 11;
            case 14: return 12;
            case 15: return 13;
            case 16: return 14;
            case 17: return 15;
            case 18: return 16;
            case 19: return 17;
            case 20: return 18;
            case 21: return 19;
            case 22: return 20;
            case 23: return 21;
            default: return 22;
        }
    }
    //industrytype取该字段的最后一位数字作为权重
    public int industrytype_score(String industrytype) {
        String last_char = industrytype.substring(industrytype.length()-1, industrytype.length()) ;
        return Integer.parseInt(last_char);
    }
}