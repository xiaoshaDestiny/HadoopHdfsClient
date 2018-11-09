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

	// 拷贝本地文件到HDFS文件系统上面
	@Test
	public void copyFileToHDFS() throws Exception {
		// 0 获取配置信息
		Configuration configuration = new Configuration();
//		configuration.set("fs.defaultFS", "hdfs://hadoop102:8020");
		// 1 获取文件系统
//		FileSystem fs = FileSystem.get(configuration);
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "root");
		// 2 拷贝本地数据到集群
		fs.copyFromLocalFile(new Path("e:/flow.txt"), new Path("/user/root/input/flow.txt"));
		// 3 关闭fs
		fs.close();
		System.out.println("copyFileToHDFS() ... over");
	}

	// 获取文件系统
	@Test
	public void getFileSystem() throws Exception {
		Configuration configuration = new Configuration();// 创建配置信息对象
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "root");// 1 获取文件系统
		System.out.println(fs.toString());// 2打印文件系统
		fs.close();// 3 关闭资源
		System.out.println("getFileSystem() ... over");
	}

	// 上传文件 把文件从本地剪切到HDFS上
	@Test
	public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {

		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "root");
		fs.copyFromLocalFile(true, new Path("e:/hello3.txt"), new Path("/user/root/"));
		fs.close();
		System.out.println("putFileToHDFS() ... over");
	}

	// HDFS文件下载
	@Test
	public void downloadFileFromHDFS() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
		fs.copyToLocalFile(false, new Path("/hello1.txt"), new Path("e:/hello1.txt"), true);
		fs.close();
		System.out.println("downloadFileFromHDFS() ... over");
	}

	// 在HDFS上创建目录
	// 如果没有加根目录 会在 /user/root目录下里面创建目录
	@Test
	public void mkdirAtHDFS() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
		fs.mkdirs(new Path("/xiaosha"));
		fs.close();
		System.out.println("mkdirAtHDFS() ... over");
	}

	// 删除文件或者文件夹的操作
	@Test
	public void deleteAtHDFS() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
		fs.delete(new Path("/user/root/output"), true);
		fs.close();
		System.out.println("deleteAtHDFS() ... over");
	}

	// 文件名称更改 只是支持吧文件名更改了 路径不支持更改
	@Test
	public void renameAtHDFS() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");

		// 前一个path是HDFS上现在有的文件 后一个是想改为什么的文件名称
		fs.rename(new Path("/hello1.txt"), new Path("/aaa.txt"));
		fs.close();
		System.out.println("renameAtHDFS() ... over");
	}

	// HDFS文件系统的详情查看
	@Test
	public void resdListFiles() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
		// 拿到一个在("/")目录下所有文件的集合(不包含目录)
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

		// 遍历这个集合
		while (listFiles.hasNext()) {
			LocatedFileStatus status = listFiles.next();
			System.out.println(status.getPath().getName()); // 输出文件名称
			System.out.println(status.getLen()); // 文件长度
			System.out.println(status.getPermission()); // 文件权限
			System.out.println(status.getGroup()); // 文件所在组
			System.out.println(status.getOwner()); // 文件所有者

			// 如果是一个大文件 就循环遍历出这个文件在那几个主机上面
			BlockLocation[] blockLocations = status.getBlockLocations();
			for (BlockLocation blockLocation : blockLocations) {
				String[] hosts = blockLocation.getHosts();
				// 循环输出主机名称
				for (String host : hosts) {
					System.out.println(host);
				}
			}
			System.out.println("======= a file end =======");
		}
		fs.close();
		System.out.println("resdListFiles() ... over");
	}

	// HDFS文件系统上判断是文件还是文件夹
	@Test
	public void findAtHDFS() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "root");
		FileStatus[] listStatus = fs.listStatus(new Path("/user/root"));
		for (FileStatus fileStatus : listStatus) {
			if (fileStatus.isFile()) {
				System.out.println(fileStatus.getPath().getName() + " 它是文件");
			}
			if (fileStatus.isDirectory()) {
				System.out.println(fileStatus.getPath().getName() + " 它是目录");
			}
		}
		fs.close();
		System.out.println("findAtHDFS() ... over");
	}

}
