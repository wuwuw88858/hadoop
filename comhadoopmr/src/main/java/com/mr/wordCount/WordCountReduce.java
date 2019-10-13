package com.mr.wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhijie
 * @date 2019-10-04 21:56
 *
 * (dear, 1)
 *
 * (dear, 1000)
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, LongWritable> {


    public void reducer(Text text, Iterable<IntWritable> vals, Context context) {
        int sum = 0;
        for(IntWritable val : vals) {
            sum += val.get();
        }
        try {
            context.write(text, new LongWritable(sum));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
