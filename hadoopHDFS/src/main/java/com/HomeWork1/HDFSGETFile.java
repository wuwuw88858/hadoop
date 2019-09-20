package com.HomeWork1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * 向HDFS读取文件到本地磁盘中
 * */
public class HDFSGETFile {

    public static void main(String[] args) {
        Configuration conf = new Configuration();   //生成配置
        String HDFS_PATH = "hdfs://node01:8082/datatest/data.xml";
        String LOCAL_PATH = "F:/bigData/课程/作业/dataFromHDFS.txt";    //本地文件存在的话会被覆盖

        try {
            //通过fileSystem创建对象（uri, 配置信息）
            FileSystem fileSystem = FileSystem.get(URI.create(HDFS_PATH), conf);

            //通过fileSystem对象打开FSDatainputStream读取对象信息。
            FSDataInputStream fsDataInputStream = fileSystem.open(new Path(HDFS_PATH));

            //创建本地输出流的缓冲流，读取文件信息
            BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(LOCAL_PATH));

            //将HDFS下的文件以字节流的形式读到本地中
            IOUtils.copyBytes(fsDataInputStream, out, 4096, true);

            System.out.println("读取成功！");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
