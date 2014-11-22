package com.lorin.hadoop.lucene;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.classifier.bayes.BayesParameters;

public class UserClassifyDriver {

	public static final String HDFS = "hdfs://192.168.137.102:9000";
	
	public static void main(String[] args) throws Exception {
		try{
			System.out.println("start job.........");
			String inputStr = HDFS + "/usr/hdfs/lucene/output3";
			String outputStr = HDFS + "/usr/hdfs/lucene/output4";
			String model = HDFS + "/usr/hdfs/lucene/model-cbayes";
			//set bayes params
			BayesParameters params = new BayesParameters();
			params.set("classifierType", "cbayes");
			params.set("alpha_i", "1.0");
			params.set("defaultcat", "unknow");
			params.setGramSize(1);
//			params.setBasePath(HDFS+args[2]);
			params.setBasePath(model);
			
			//set configation
			Configuration conf = new Configuration();
			conf.set("bayes.parameters", params.toString());
			
			//create job
			Job job = new Job(conf,"userClassifier");
			job.setJarByClass(UserClassifyDriver.class);
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			
			job.setMapperClass(UserClassifyMapper.class);
			job.setReducerClass(UserClassifyReduce.class);
			
			
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
//			Path input = new Path(HDFS+args[0]);
//			Path output = new Path(HDFS+args[1]);
			Path input = new Path(inputStr);
			Path output = new Path(outputStr);
			FileInputFormat.addInputPath(job,input );
			FileOutputFormat.setOutputPath(job, output);
			
			FileSystem hdfs = output.getFileSystem(conf);
			if(hdfs.exists(output)){
				hdfs.delete(output);
			}
			hdfs.close();
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}catch(Exception e){
			e.printStackTrace();
			return ;
		}
	}
}
