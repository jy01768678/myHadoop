package com.lorin.hadoop.lucene;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.lorin.hadoop.lucene.inputformat.MyInputFormat;


public class TokenizeDriver {

	public static final String HDFS = "hdfs://192.168.137.102:9000";
	
	
	public static void main(String[] args) throws Exception {
		
		String input = HDFS + args[0];
		String output = HDFS + args[1];
		// set configuration
		Configuration conf = new Configuration();
		conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 4000000);    //max size of Split
		conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        
		Job job = new Job(conf,"Tokenizer");
		job.setJarByClass(TokenizeDriver.class);

	    // specify input format
		job.setInputFormatClass(MyInputFormat.class);
		
        //  specify mapper
		job.setMapperClass(TokenizeMapper.class);
		
		// specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// specify input and output DIRECTORIES 
		Path inPath = new Path(input);
		Path outPath = new Path(output);
		try {                                            //  input path
			FileSystem fs = inPath.getFileSystem(conf);
			FileStatus[] stats = fs.listStatus(inPath);
			for(int i=0; i<stats.length; i++)
				FileInputFormat.addInputPath(job, stats[i].getPath());
		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		}			
        FileOutputFormat.setOutputPath(job,outPath);     //  output path

		// delete output directory
		try{
			FileSystem hdfs = outPath.getFileSystem(conf);
			if(hdfs.exists(outPath))
				hdfs.delete(outPath);
			hdfs.close();
		} catch (Exception e){
			e.printStackTrace();
			return ;
		}
		
		//  run the job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
