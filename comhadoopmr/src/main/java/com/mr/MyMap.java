package com.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 1、map程序，继承Mapper类<iKey, iVal, oKey, oVal>
 *     iKey:输入的key的类型
 *     iVal:输入的val的类型
 *     oKey:输出的key的类型
 *     oVal:输出的val的类型
 *
 *     从HDFS中输入，输出到Reduce()中
 *  需求：
 *    输入(Key, Val)：
 *      key类型：LongWritable:当前所读行的行首相对于分片的字节偏移量，需要long类型，hadoop包装成序列化类型,即LongWritable
 *      val类型：Text:读取行的内容，需要String,hadoop包装成序列化类型即Text
 *    例子：
 *      （0, lineContext1）
 *       (0, lineContext2)
 *       (0, lineContext3)
 *       .....
 *    输出（key， val）:
 *      key类型：text:读取的单词
 *      val类型：IntWritable:单词输出的个数
 *     例子：
 *     （word, 1）
 *     (word, 1)
 *     (word, 1)
 *     (word, 1)
 *
 *      * */
public class MyMap extends Mapper<LongWritable, Text, Text, IntWritable>{

    InputFormat

    /**
     * 针对于split中每一行内容，都会执行一次map任务
     * 每次执行一次map任务的时候，就会将每行的内容通过\t(一个tab)分割出一个个单词
     * 最终每个单词都形成一个 kv对 （word, 1）
     * 输出的值会被写到磁盘文件中。
     *
     *
     * */
    private Text text = new Text();
    private IntWritable intWritable = new IntWritable(1);
    public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {

        //当前行
        String line = val.toString();
        System.out.println("读取行：" + line);
        //使用 \t分割一行的内容，
        String[] words = line.split("\t");
        //将每一个单词以（word， 1）的形式输出到reduce程序中

        for (String word : words) {
            text.set(word); //将单词包装成可序列化，即text类型
        context.write(text, intWritable);
        }
    }
}
