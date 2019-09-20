package com.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reduce程序，继承Reducer父类<iKey, iVal, oKey, oVal>
 *     iKey:接收输入的key类型
 *     iVal:接收输入的val类型
 *  ------以上类型必须以map程序输出的类型一致
 *    oKey:输出的key类型
 *    oVal:输出的val类型
 *    最终输出到HDFS指定的文件中
 *
 *    需求：map任务传入
 *    (word, 1)
 *    (word, 1)
 *    (word, 1)
 *    key为Text,即String的可序列化类型
 *    val为IntWritable,即int的可序列化类型
 *
 *    reduce传出为每个单词的总个数，即
 *    （word, 10）
 *    (word2, 20)
 *    ....
 *    key为Text类型
 *    val为IntWritable类型
 *
 * */
public class MyReduce extends Reducer<Text, IntWritable, Text, Writable>{

    /**
     * 在reduce任务中，相同的一组kv对会执行一次reduce方法
     * reduce任务汇聚众多的 kv对 如：
     *
     *     (word, 1)
     *     (word, 1)
     *     (word, 1)
     *     (word, 1)
     *     (word, 1)
     *     (word, 1)
     *
     *相同的键值对会被分成一组。merge成<word, Iterable<1, 1, 1, 1, 1>> --> <key, list of val>
     *     每组的<key, list of val> 会调用一次reduce()方法
     */

    public void reduce(Text key, Iterable<IntWritable> vals, Context context) throws IOException, InterruptedException {
        int sum = 0;    //累计单词出现的次数
        System.out.println("key -->" + key);
        System.out.println("vals -->" + vals);
        for (IntWritable val : vals) {
            sum += val.get();   //从中获取值
        }

        //将单词，单词计算的总数作为结果输出在文件中
        context.write(key, new IntWritable(sum));
    }
}
