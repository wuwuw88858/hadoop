package com.mr;

import org.apache.hadoop.mapreduce.InputFormat;

import java.net.MalformedURLException;
import java.net.URL;

public class TestMain {

    public static void main(String[] args) throws MalformedURLException {
        pojo p = new pojo();
        p.compareTo(1);
       String url = "http://auto.ifeng.com/roll/20111212/729164.html";
       URL urlS = new URL(url);
        String host = urlS.getHost();
        System.out.println(host);

    }
}
