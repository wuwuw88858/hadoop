package com.mr.wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhijie
 * @date 2019-10-04 21:42
 *
 * 输入的到map中的值
 *  kin: 每行的偏移量
 *  vin: 每行的的内容 如：dear    dear    dear    dear
 *
 *  kout：输出的每个单词 -> dear
 *  vout: 1 记录单词的个数
 */
public class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    IntWritable intWritable = new IntWritable(1);
    public void map(LongWritable longWritable, Text text, Context context) {
        String[] words = text.toString().split("\t");

        for(String word : words) {
            try {
                context.write(new Text(word), intWritable); //（dear, 1） （dear, 1） （dear, 1） （dear, 1）形式输出
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
