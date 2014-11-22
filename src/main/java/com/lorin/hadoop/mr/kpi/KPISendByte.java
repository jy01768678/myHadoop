package com.lorin.hadoop.mr.kpi;

import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class KPISendByte {

	public static class KPISBMapper extends MapReduceBase implements Mapper<Object, Text, Text, LongWritable> {

		private LongWritable one = new LongWritable(1);
        private Text word = new Text();
        
		public void map(Object key, Text value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			 KPI kpi = KPI.filterSB(value.toString());
             if (kpi.isValid() && StringUtils.isNotBlank(kpi.getBody_bytes_sent())) {
                try {
					word.set(kpi.getTime_local_Date_day());
					one.set(Long.valueOf(kpi.getBody_bytes_sent()));
					output.collect(word, one);
				} catch (ParseException e) {
					e.printStackTrace();
				}
             }
		}
	}
	
	public static class KPISBReducer extends MapReduceBase implements Reducer<Text,LongWritable,Text,LongWritable>{

		private LongWritable result = new LongWritable(1);
		
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			long sum = 0l;
			while (values.hasNext()) {
                sum += values.next().get();
            }
			result.set(sum);
			output.collect(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
        String input = "hdfs://192.168.1.102:9000/usr/hdfs/log_kpi/data";
        String output = "hdfs://192.168.1.102:9000/usr/hdfs/log_kpi/homework";

        JobConf conf = new JobConf(KPISendByte.class);
        conf.setJobName("KPISendByte");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(LongWritable.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        conf.setMapperClass(KPISBMapper.class);
        conf.setCombinerClass(KPISBReducer.class);
        conf.setReducerClass(KPISBReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
        System.exit(0);
    }
}
