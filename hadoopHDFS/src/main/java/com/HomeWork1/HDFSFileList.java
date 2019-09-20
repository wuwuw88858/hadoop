package com.HomeWork1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;

/**
 * 列出多级目录名称和目录下的文件名称
 * */
public class HDFSFileList {

    public static void main(String[] args) {
        String HDFS_PATH = "hdfs://node01:8082";
        Configuration conf = new Configuration();
        try {
            FileSystem fileSystem = FileSystem.get(URI.create(HDFS_PATH), conf);
            System.out.println(fileSystem.getUri());

            //获取hdfs://node01:8082/datatest下的所有文件。true代表递归
            RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/datatest"), true);
            while (files.hasNext()) {
                LocatedFileStatus file = files.next();

                System.out.println("文件名称 :" + file.getPath().getName());
                System.out.println("文件路径 :" + file.getPath());
                System.out.println("文件大小：" + file.getBlockSize());
                System.out.println("文件拥有者：" + file.getOwner());
                System.out.println("文件副本数：" + file.getReplication());
                System.out.println(file.getPath());
                //获取块信息
                BlockLocation[] fileBolcks = file.getBlockLocations();
                for (BlockLocation fileBlock : fileBolcks) {
                    System.out.println("块大小：" + fileBlock.getLength());
                    System.out.println("块偏移量：" + fileBlock.getOffset());
                    String[] names = fileBlock.getNames();
                    for (String name: names) {
                        System.out.println("块所在ip + 端口：" + name);
                    }
                    String[] hosts = fileBlock.getHosts();
                    for (String host : hosts) {
                        System.out.println("块所在dn:" + host);
                    }
                }
                System.out.println("------------------------");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
