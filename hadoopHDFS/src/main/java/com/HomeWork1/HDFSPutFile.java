package com.HomeWork1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;


public class HDFSPutFile {

    public static void main(String[] args) {
        String HDFS_PATH="hdfs://node01:8082/datatest/data.xml";
        String LOCAL_PATH="F:/bigData/课程/作业/data.xml";

        //创建一个输入流
        InputStream in = null;
        try {
            //在本地生成文件输入流对本地的文件读取，并形成缓冲流
            in = new BufferedInputStream(new FileInputStream(LOCAL_PATH));
            //对信息读取进行配置
            Configuration con = new Configuration();

            System.out.println(con);
            //通过hadoop的fileSystetem（远程地址，信息配置）创建对象。通过文件系统对象创建出一个输出流写入数据
            FileSystem fileSystem = FileSystem.get(URI.create(HDFS_PATH), con);
            OutputStream out= fileSystem.create(new Path(HDFS_PATH));
            //复制本地文件到hdfs目录中
            IOUtils.copyBytes(in, out, 4096, true);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
