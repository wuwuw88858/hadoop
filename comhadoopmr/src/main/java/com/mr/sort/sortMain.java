package com.mr.sort;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author zhijie
 * @date 2019-10-05 13:53
 */
public class sortMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        //job实例
        Job job = Job.getInstance(conf, sortMain.class.getSimpleName());

        //设置jar
        job.setJarByClass(sortMain.class);

        //设置输入/输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //设置Map  map输出key val
        job.setMapperClass(com.mr.sort.sortMap.class);
        job.setMapOutputKeyClass(Person.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置reduce reduce输出的key val类型
        job.setReducerClass(com.mr.sort.sortReduce.class);
        job.setOutputKeyClass(Person.class);
        job.setOutputValueClass(Person.class);

        //设置reduce个数
        job.setNumReduceTasks(1);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交作业
        job.waitForCompletion(true);
    }
}
