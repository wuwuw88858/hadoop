package com.mr.topN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author zhijie
 * @date 2019-10-05 21:53
 */
public class GoodsMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //设置conf
        Configuration conf = new Configuration();

        //生成job实例
        Job job = Job.getInstance(conf, GoodsMain.class.getSimpleName());

        //设置输入/是输出类型
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //设置Mapper类 map的输出kv对
        job.setMapperClass(com.mr.topN.GoodsMapper.class);
        job.setMapOutputKeyClass(Goods.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        //设置Reduce类
        job.setReducerClass(com.mr.topN.GoodsReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        //自定义分区
        job.setPartitionerClass(com.mr.topN.GoodsPartition.class);
        //自定义分组
        job.setGroupingComparatorClass(com.mr.topN.GoodsGroup.class);

        //提交作业
        job.waitForCompletion(true);
    }
}
