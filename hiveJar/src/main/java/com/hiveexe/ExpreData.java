package com.hiveexe;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.lang.reflect.Array;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author zhijie
 * @date 2019-10-03 11:06
 */
public class ExpreData extends UDF {

    int num = 0;
    boolean firstFlag = false;
    boolean secondFlag = false;
    int a = 0;
    //["2018-07-02","2018-09-01","2017-06-29"]
    public int evaluate(List<String> dates, String date1, String date2) {
        for (String date : dates) {
            if (changToTime(date) > changToTime(date1)) {
                if (changToTime(date) > changToTime(date2)) {
                    secondFlag = true;
                }  else {
                    firstFlag = true;
                }
            }
        }
        num =  secondFlag == true && firstFlag == true ? (num = 1) : (num = 0);
        return num;
    }

    private Long changToTime(String date) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date date1 = simpleDateFormat.parse(date);
            long time = date1.getTime();
            return time;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

}
