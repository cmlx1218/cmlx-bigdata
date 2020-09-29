package com.cmlx.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {

    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {
        //1、获取文件系统
        Configuration configuration = new Configuration();
        //2、配置在集群上运行
//        configuration.set("fs.defaultFS","hdfs://slave102:9000");
//        FileSystem fileSystem = FileSystem.get(configuration);
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:8020"), configuration, "hdfs");
        //3、创建目录
        fileSystem.mkdirs(new Path("/testHdfs/mkdir/cmlx"));
        //4、关闭资源
        fileSystem.close();
    }

    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        //1、获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:8020"), configuration, "hdfs");
        //2、上传文件
        fileSystem.copyFromLocalFile(new Path("D:/document/aimy/环境安装/bigdata集群/Ambari/ambari2.7.4安装手册"), new Path("/testHdfs/mkdir/cmlx/ambari2.7.4安装手册"));
        //3、关闭资源
        fileSystem.close();
    }

    @Test
    public void testCopyToLocalFile() throws URISyntaxException, IOException, InterruptedException {
        //1、获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:8020"), configuration, "hdfs");
        //2、下载文件
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fileSystem.copyToLocalFile(false, new Path("/testHdfs/mkdir/cmlx/ambari2.7.4安装手册"), new Path("D:/ambari2.7.4安装手册"), true);
    }

    @Test
    public void testDelete() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:8020"), configuration, "hdfs");

        // 2 执行删除
        fs.delete(new Path("/testHdfs/mkdir/cmlx/ambari2.7.4安装手册"), true);

        // 3 关闭资源
        fs.close();
    }

}
