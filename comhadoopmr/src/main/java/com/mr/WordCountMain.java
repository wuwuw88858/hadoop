package com.mr;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * mr程序主入口
 * */
public class WordCountMain {

    public static void main(String[] args) {


        //在配置文件中输入输入的路径以及输出的路径，如果值少于2，则不执行程序
//        if (args.length < 2) {
//            System.out.println("请输入 输入文件路径以及文件输出路径");
//            System.exit(0);
//        }

        //读取配置文件信息
        Configuration conf = new Configuration();
       String mainClassName = WordCountMain.class.getSimpleName();   //读取主函数的名称，作为job的名称
        //生成job实例。
        try {
            //生成job实例
            Job job = Job.getInstance(conf, mainClassName);
            //设置jar包，参数是包含main方法的类
            job.setJarByClass(WordCountMain.class);

            //设置job的输入/输出格式
            //mr的默认输入格式：TextInputFormat
            //mr默认的输出格式：TextOutputFormat
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            //设置map输入/输出路径
          //
            FileInputFormat.setInputPaths(job, new Path("hdfs://node01:8082/MRFileTest/lineWord.txt"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://node01:8082/MRFileTest/MRresult"));

            //设置处理map任务的类
            //设置处理reduce任务的类
            job.setMapperClass(MyMap.class);
            job.setReducerClass(MyReduce.class);

            //设置输出map的kv对类型
            /**注意！！！！！！！！：
             * 如果map任务中输出的kv对的类型和reduce任务中输入的kv对的类型一致的话。直接设置reduce输出的kv对类型也可以
             *
             * */
            //此处设置map输出的kv对类型，一定要和map任务中的输出kv对的类型相同。即（word, 1）。否则程序出错
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            //设置输出的kv对类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //提交作业
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
