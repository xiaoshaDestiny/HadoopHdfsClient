package com.learn.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

//用IO流去操作HDFS文件系统
public class HdfsIOClient {

	// 上传文件到HDFS文件系统
	@Test
	public void putFileToHDFS() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");

		// 获取输入流
		FileInputStream fis = new FileInputStream(new File("e:/hello.txt"));
		// 获取输出流
		FSDataOutputStream fos = fs.create(new Path("/hello.txt"));
		// 流对拷
		IOUtils.copyBytes(fis, fos, conf);

		// 关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		fis.close();
		fos.close();

		// 输出结束提示
		System.out.println("putFileToHDFS() ... over");
	}

	// 下载HDFS上的文件到客户端本地
	@Test
	public void downloadFileToClient() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");

		// 获取输入流
		FSDataInputStream fis = fs.open(new Path("/hello.txt"));
		// 获取输出流
		FileOutputStream fos = new FileOutputStream("e:/hello1.txt");
		// 流对拷
		IOUtils.copyBytes(fis, fos, conf);

		// 关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		fis.close();
		fos.close();

		// 输出结束提示
		System.out.println("downloadFileToClient() ... over");
	}

	// 定位文件读取 一个大文件在HDFS上被分为了两个块 只想读取其中一块怎么操作如下
	@Test
	public void resdFilePart1() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
		// 读取一个大文件的第一块
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
		FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part1"));
		byte[] buf = new byte[1024];
		for (int i = 0; i < 1024 * 128; i++) {
			fis.read(buf);
			fos.write(buf);
		}
		// 关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		fis.close();
		fos.close();
		// 输出结束提示
		System.out.println("downloadFileToClient() ... over");

	}

	// 下载文件第二快
	@Test
	public void readFilePart2() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
		fis.seek(1024 * 1024 * 128);
		FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part2"));
		IOUtils.copyBytes(fis, fos, conf);
		// 关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		fis.close();
		fos.close();
		// 输出结束提示
		System.out.println("downloadFileToClient() ... over");
	}

}
