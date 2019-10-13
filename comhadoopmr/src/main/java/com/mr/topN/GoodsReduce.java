package com.mr.topN;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhijie
 * @date 2019-10-05 21:37
 */
public class GoodsReduce extends Reducer<Goods, DoubleWritable, Text, DoubleWritable> {

    //只写入前两条
    public void reduce(Goods key, Iterable<DoubleWritable> vals, Context context) {
        System.out.println("key :" + key);
        System.out.println("val:" + vals);
        int sum = 0;
        for (DoubleWritable val : vals) {
            if (sum < 2) {
                String kout = key.getUserId() + " " + key.getDatetime();
                try {
                    context.write(new Text(kout), val);
                    sum ++;
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else {
                break;
            }
        }
    }
}
