package com.lorin.hadoop.test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FileStatusTest {

	public static final String HDFS = "hdfs://192.168.137.102:9000";

	private static final double SPLIT_SLOP = 1.1; // 10% slop

	public static void testFileStatus() {
		try {
//			String path = HDFS + "/usr/hdfs/mahout/synthetic_control.data";
			String path = HDFS + "/usr/hdfs/lucene/digital/MP3/MP310";
			// String path = HDFS + "/usr/hdfs/recommend";
			JobConf conf = new JobConf(FileStatusTest.class);
			conf.addResource("classpath:/hadoop/core-site.xml");
			conf.addResource("classpath:/hadoop/hdfs-site.xml");
			conf.addResource("classpath:/hadoop/mapred-site.xml");
			FileSystem fs = FileSystem.get(URI.create(path), conf);
			// FileStatus对象封装了文件的和目录的额元数据，包括文件长度、块大小、权限等信息
			FileStatus fileStatus = fs.getFileStatus(new Path(path));
			if (!fileStatus.isDir()) {
				System.out.println("文件路径：" + fileStatus.getPath());
				System.out.println("块的大小：" + fileStatus.getBlockSize());
				System.out.println("文件所有者：" + fileStatus.getOwner() + ":"
						+ fileStatus.getGroup());
				System.out.println("文件权限：" + fileStatus.getPermission());
				System.out.println("文件长度：" + fileStatus.getLen());
				System.out.println("备份数：" + fileStatus.getReplication());
				System.out.println("修改时间：" + fileStatus.getModificationTime());
//				BlockLocation[] blc = fs.getFileBlockLocations(fileStatus, 0,
//						fileStatus.getLen());
//				for (BlockLocation bl : blc) {
//					System.out.println(bl.getHosts().length);
//					System.out.println("hosts:" + bl.getHosts()[0] + "\t"
//							+ bl.getHosts()[1]);
//					System.out.println("names:" + bl.getNames()[0] + "\t"
//							+ bl.getNames()[1]);
//				}

				// 测试分块分到哪个 map上了
				List<InputSplit> splits = new ArrayList<InputSplit>();
				long length = fileStatus.getLen();
				long bytesRemaining = length;
//				long blockSize = fileStatus.getBlockSize();
				long splitSize = 67108864;// 64M
				BlockLocation[] blkLocations = fs.getFileBlockLocations(
						fileStatus, 0, length);
//				System.out.println(blkLocations[blkLocations.length-1]);
				while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
					int blkIndex = getBlockIndex(blkLocations, length
							- bytesRemaining);
					splits.add(new FileSplit(new Path(path), length - bytesRemaining,
							splitSize, blkLocations[blkIndex].getHosts()));
					bytesRemaining -= splitSize;
				}
				if (bytesRemaining != 0) {
			          splits.add(new FileSplit(new Path(path), length-bytesRemaining, bytesRemaining, 
			                     blkLocations[blkLocations.length-1].getHosts()));
			        }
				for(InputSplit is : splits){
					System.out.println(is.getLocations().length);
				}
			} else {
				System.out.println("文件路径：" + fileStatus.getPath());
				System.out.println("块的大小：" + fileStatus.getBlockSize());
				System.out.println("文件所有者：" + fileStatus.getOwner() + ":"
						+ fileStatus.getGroup());
				System.out.println("文件权限：" + fileStatus.getPermission());
				System.out.println("文件长度：" + fileStatus.getLen());
				System.out.println("备份数：" + fileStatus.getReplication());
				System.out.println("修改时间：" + fileStatus.getModificationTime());
				System.out.println("这个目录下包含以下文件或目录：");
				for (FileStatus fileSs : fs.listStatus(new Path(path))) {
					System.out.println(fileSs.getPath());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	protected static int getBlockIndex(BlockLocation[] blkLocations, long offset) {
		for (int i = 0; i < blkLocations.length; i++) {
			// is the offset inside this block?
			if ((blkLocations[i].getOffset() <= offset)
					&& (offset < blkLocations[i].getOffset()
							+ blkLocations[i].getLength())) {
				return i;
			}
		}
		BlockLocation last = blkLocations[blkLocations.length - 1];
		long fileLength = last.getOffset() + last.getLength() - 1;
		throw new IllegalArgumentException("Offset " + offset
				+ " is outside of file (0.." + fileLength + ")");
	}

	public static void main(String[] args) {
		testFileStatus();
	}

}
