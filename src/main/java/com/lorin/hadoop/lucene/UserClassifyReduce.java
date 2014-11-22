package com.lorin.hadoop.lucene;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserClassifyReduce extends Reducer<Text,IntWritable,NullWritable,Text>{

	private Text outValue = new Text();

	public void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		int num = 0;
		for(IntWritable value : values){
			num += value.get();
		}
		outValue.set(key.toString() + "|" + num);
		context.write(NullWritable.get(), outValue);
	}
	
	
}
