package com.HomeWork1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class HDFSGetDire {

    public static void main(String[] args) {
        getDirAndFileName(new Path("/"));
    }
    //获取某目录下的所有目录名称
    public static void outPutFile(Path path) {
        String HDFS_PATH = "hdfs://node01:8082";
        Configuration conf = new Configuration();
        try {
            FileSystem fileSystem = FileSystem.get(URI.create(HDFS_PATH), conf);

            //获取文件列表
            FileStatus[] files = fileSystem.listStatus(path);

            //
           Path[] paths =  FileUtil.stat2Paths(files);
           if (paths.length == 0) {
               System.out.println(path + "目录没有文件");
           }
           for (Path path1 : paths) {
               System.out.println(path + "下的文件名称：" + path1.getName()); //输出/下的目录名称
           }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //获取每级目录
    public static void getDirAndFileName(Path path) {
        String HDFS_PATH = "hdfs://node01:8082";
        Configuration conf = new Configuration();
        try {
            FileSystem fileSystem = FileSystem.get(URI.create(HDFS_PATH), conf);
            FileStatus[] files = fileSystem.listStatus(path);
            //展示文件

            for (FileStatus file : files) {
                if (file.isDirectory()) {
                    System.out.println(path + "下的目录：" + file.getPath());

                    outPutFile(file.getPath()); //输出文件名称
                    System.out.println("-----------");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
