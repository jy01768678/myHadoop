package com.lorin.hadoop.mr.kpi;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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

public class KPIBrowserPersent {
	public static class KPIBrowserPersentMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            KPI kpi = KPI.filterBroswer(value.toString());
            if (kpi.isValid() && StringUtils.isNotBlank(kpi.getUaType())) {
                word.set(kpi.getUaType());
                output.collect(word, one);
            }
        }
    }
	
	public static class KPIBrowserPersentCombiner extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            result.set(sum);
            output.collect(key, result);
        }
    }

    public static class KPIBrowserPersentReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {
        private Text result = new Text();
        BigDecimal count = new BigDecimal(2909325);
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            double sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            result.set(String.valueOf(new BigDecimal(sum).divide(count, 7,BigDecimal.ROUND_HALF_UP)));
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        String input = "hdfs://192.168.1.102:9000/usr/hdfs/log_kpi/data";
        String output = "hdfs://192.168.1.102:9000/usr/hdfs/log_kpi/homework";

        JobConf conf = new JobConf(KPIBrowser.class);
        conf.setJobName("KPIBrowser");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(KPIBrowserPersentMapper.class);
        conf.setCombinerClass(KPIBrowserPersentCombiner.class);
        conf.setReducerClass(KPIBrowserPersentReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
        System.exit(0);
    }
}
