package com.HomeWork1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;

/**
 *
 *列出某一个文件夹下的所有文件
 * */
public class HDFSPathList {

    public static void main(String[] args) {
        String HDFS_PATH = "hdfs://node01:8082";
        Configuration con = new Configuration();
        try {
            FileSystem fileSystem = FileSystem.get(URI.create(HDFS_PATH), con); //打开HDFS，并返回fileSystem对象
            //返回/datatest下的文件名称以及子文件目录下的文件名称
            RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/datatest"), true);
          while (files.hasNext()) {
               LocatedFileStatus file = files.next();
               System.out.println(file.getPath().getParent().getName() + "文件下的文件名称：" + file.getPath().getName());
           }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
