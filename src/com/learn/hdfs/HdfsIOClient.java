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

//��IO��ȥ����HDFS�ļ�ϵͳ
public class HdfsIOClient {

	// �ϴ��ļ���HDFS�ļ�ϵͳ
	@Test
	public void putFileToHDFS() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");

		// ��ȡ������
		FileInputStream fis = new FileInputStream(new File("e:/hello.txt"));
		// ��ȡ�����
		FSDataOutputStream fos = fs.create(new Path("/hello.txt"));
		// ���Կ�
		IOUtils.copyBytes(fis, fos, conf);

		// �ر���Դ
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		fis.close();
		fos.close();

		// ���������ʾ
		System.out.println("putFileToHDFS() ... over");
	}

	// ����HDFS�ϵ��ļ����ͻ��˱���
	@Test
	public void downloadFileToClient() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");

		// ��ȡ������
		FSDataInputStream fis = fs.open(new Path("/hello.txt"));
		// ��ȡ�����
		FileOutputStream fos = new FileOutputStream("e:/hello1.txt");
		// ���Կ�
		IOUtils.copyBytes(fis, fos, conf);

		// �ر���Դ
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		fis.close();
		fos.close();

		// ���������ʾ
		System.out.println("downloadFileToClient() ... over");
	}

	// ��λ�ļ���ȡ һ�����ļ���HDFS�ϱ���Ϊ�������� ֻ���ȡ����һ����ô��������
	@Test
	public void resdFilePart1() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
		// ��ȡһ�����ļ��ĵ�һ��
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
		FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part1"));
		byte[] buf = new byte[1024];
		for (int i = 0; i < 1024 * 128; i++) {
			fis.read(buf);
			fos.write(buf);
		}
		// �ر���Դ
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		fis.close();
		fos.close();
		// ���������ʾ
		System.out.println("downloadFileToClient() ... over");

	}

	// �����ļ��ڶ���
	@Test
	public void readFilePart2() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
		fis.seek(1024 * 1024 * 128);
		FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part2"));
		IOUtils.copyBytes(fis, fos, conf);
		// �ر���Դ
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		fis.close();
		fos.close();
		// ���������ʾ
		System.out.println("downloadFileToClient() ... over");
	}

}
