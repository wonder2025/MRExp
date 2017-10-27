package com.hive;

public class test {
    public static  void main(String[] args){
        String city_no="10";
        String char1 = city_no.substring(0, 1) ;
        int a = Integer.parseInt(char1);
        String char2 = city_no.substring(1, 2) ;
        int b = Integer.parseInt(char2);

        System.out.println(a +"-------"+b);
    }


}
