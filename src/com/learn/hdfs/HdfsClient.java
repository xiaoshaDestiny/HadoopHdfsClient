package com.learn.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

public class HdfsClient {

	// ���������ļ���HDFS�ļ�ϵͳ����
	@Test
	public void copyFileToHDFS() throws Exception {
		// 0 ��ȡ������Ϣ
		Configuration configuration = new Configuration();
//		configuration.set("fs.defaultFS", "hdfs://hadoop102:8020");
		// 1 ��ȡ�ļ�ϵͳ
//		FileSystem fs = FileSystem.get(configuration);
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "root");
		// 2 �����������ݵ���Ⱥ
		fs.copyFromLocalFile(new Path("e:/flow.txt"), new Path("/user/root/input/flow.txt"));
		// 3 �ر�fs
		fs.close();
		System.out.println("copyFileToHDFS() ... over");
	}

	// ��ȡ�ļ�ϵͳ
	@Test
	public void getFileSystem() throws Exception {
		Configuration configuration = new Configuration();// ����������Ϣ����
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "root");// 1 ��ȡ�ļ�ϵͳ
		System.out.println(fs.toString());// 2��ӡ�ļ�ϵͳ
		fs.close();// 3 �ر���Դ
		System.out.println("getFileSystem() ... over");
	}

	// �ϴ��ļ� ���ļ��ӱ��ؼ��е�HDFS��
	@Test
	public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {

		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "root");
		fs.copyFromLocalFile(true, new Path("e:/hello3.txt"), new Path("/user/root/"));
		fs.close();
		System.out.println("putFileToHDFS() ... over");
	}

	// HDFS�ļ�����
	@Test
	public void downloadFileFromHDFS() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
		fs.copyToLocalFile(false, new Path("/hello1.txt"), new Path("e:/hello1.txt"), true);
		fs.close();
		System.out.println("downloadFileFromHDFS() ... over");
	}

	// ��HDFS�ϴ���Ŀ¼
	// ���û�мӸ�Ŀ¼ ���� /user/rootĿ¼�����洴��Ŀ¼
	@Test
	public void mkdirAtHDFS() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
		fs.mkdirs(new Path("/xiaosha"));
		fs.close();
		System.out.println("mkdirAtHDFS() ... over");
	}

	// ɾ���ļ������ļ��еĲ���
	@Test
	public void deleteAtHDFS() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
		fs.delete(new Path("/user/root/output"), true);
		fs.close();
		System.out.println("deleteAtHDFS() ... over");
	}

	// �ļ����Ƹ��� ֻ��֧�ְ��ļ��������� ·����֧�ָ���
	@Test
	public void renameAtHDFS() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");

		// ǰһ��path��HDFS�������е��ļ� ��һ�������Ϊʲô���ļ�����
		fs.rename(new Path("/hello1.txt"), new Path("/aaa.txt"));
		fs.close();
		System.out.println("renameAtHDFS() ... over");
	}

	// HDFS�ļ�ϵͳ������鿴
	@Test
	public void resdListFiles() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
		// �õ�һ����("/")Ŀ¼�������ļ��ļ���(������Ŀ¼)
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

		// �����������
		while (listFiles.hasNext()) {
			LocatedFileStatus status = listFiles.next();
			System.out.println(status.getPath().getName()); // ����ļ�����
			System.out.println(status.getLen()); // �ļ�����
			System.out.println(status.getPermission()); // �ļ�Ȩ��
			System.out.println(status.getGroup()); // �ļ�������
			System.out.println(status.getOwner()); // �ļ�������

			// �����һ�����ļ� ��ѭ������������ļ����Ǽ�����������
			BlockLocation[] blockLocations = status.getBlockLocations();
			for (BlockLocation blockLocation : blockLocations) {
				String[] hosts = blockLocation.getHosts();
				// ѭ�������������
				for (String host : hosts) {
					System.out.println(host);
				}
			}
			System.out.println("======= a file end =======");
		}
		fs.close();
		System.out.println("resdListFiles() ... over");
	}

	// HDFS�ļ�ϵͳ���ж����ļ������ļ���
	@Test
	public void findAtHDFS() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
		FileStatus[] listStatus = fs.listStatus(new Path("/user/root"));
		for (FileStatus fileStatus : listStatus) {
			if (fileStatus.isFile()) {
				System.out.println(fileStatus.getPath().getName() + " �����ļ�");
			}
			if (fileStatus.isDirectory()) {
				System.out.println(fileStatus.getPath().getName() + " ����Ŀ¼");
			}
		}
		fs.close();
		System.out.println("findAtHDFS() ... over");
	}

}
