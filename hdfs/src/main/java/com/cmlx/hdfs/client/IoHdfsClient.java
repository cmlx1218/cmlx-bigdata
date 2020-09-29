package com.cmlx.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class IoHdfsClient {

    @Test
    public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:8020"), configuration, "hdfs");

        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("D:/document/aimy/环境安装/bigdata集群/Ambari/ambari2.7.4安装手册"));

        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/testHdfs/mkdir/cmlx/ambari安装手册"));

        // 4 流对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    // 文件下载
    @Test
    public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:8020"), configuration, "hdfs");

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/testHdfs/mkdir/cmlx/ambari安装手册"));

        // 3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("D:/ambari安装手册"));

        // 4 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }


}
