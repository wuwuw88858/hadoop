package com.HDFSHomeWork2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * 使用java代码,对一个文件进行压缩

 用下三种方式

 DEFLATE
 gzip
 bzip
 * */
/**
 *
 * */
public class ComPressFile {

//    public static void main(String[] args) {
//
//        //压缩类
//        String className = "org.apache.hadoop.io.compress.BZip2Codec";
//        String LOCAL_PATH = "F:/bigData/课程/作业/HDFS/data.xml";
//        String HDFS_PATH = "hdfs://node01:8082/datatest/comPreData.xml.bz2";
//        InputStream in = null;
//        try {
//            in = new BufferedInputStream(new FileInputStream(LOCAL_PATH));  //创建文件输入流，包装成缓冲流
//            //通过反射获取压缩类对象(CompressionCodec)
//            Class<?> codecClass = Class.forName(className);
//            Configuration conf = new Configuration();
//            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
//
//            FileSystem fileSystem = FileSystem.get(URI.create(HDFS_PATH), conf);    //打开HDFS的路径，返回fileSystem对象
//            OutputStream out  = fileSystem.create(new Path(HDFS_PATH));   //打开HDFS的输出流，写入数据
//            codec.createOutputStream(out);//对输出流的数据进行压缩
//            IOUtils.copyBytes(in, out, 4096, true); //传输数据
//            System.out.println("传输成功");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }

    public static void main(String[] args) {

        //压缩类
        String className = "org.apache.hadoop.io.compress.SnappyCodec";
        String LOCAL_PATH = "F:/bigData/课程/作业/HDFS/dataFromHDFS.txt";
        String HDFS_PATH = "hdfs://node01:8082/datatest/comPreData.txt.Snappy";
        long startTime = System.currentTimeMillis();
        InputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(LOCAL_PATH));  //创建文件输入流，包装成缓冲流
            //通过反射获取压缩类对象(CompressionCodec)
            Class<?> codecClass = Class.forName(className);
            Configuration conf = new Configuration();
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

            FileSystem fileSystem = FileSystem.get(URI.create(HDFS_PATH), conf);    //打开HDFS的路径，返回fileSystem对象
            OutputStream out  = fileSystem.create(new Path(HDFS_PATH));   //打开HDFS的输出流，写入数据
            CompressionOutputStream conOut = codec.createOutputStream(out);//对输出流的写入的数据进行压缩
            IOUtils.copyBytes(in, conOut, 4096, true); //传输数据
            System.out.println("传输成功");
            long endTime = System.currentTimeMillis();
            System.out.println("运行时间：" + (endTime - startTime)/1000 + "s");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
