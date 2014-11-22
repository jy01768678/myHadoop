package com.lorin.hadoop.recommend;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.lorin.hadoop.hdfs.HdfsDAO;

public class Step5 {

	public static class Step5_FliterAndSortMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
        private final static IntWritable k = new IntWritable();
        private final static Text v = new Text();

        public void map(LongWritable key, Text values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String[] tokens = Recommend.DELIMITER1.split(values.toString());
        	k.set(Integer.parseInt(tokens[0]));
        	v.set(tokens[1]);
        	output.collect(k, v);
        }
    }
	
    public static class Step5_FliterAndSortReducer extends MapReduceBase implements Reducer<LongWritable, Text, IntWritable, Text> {
        private final static Text v = new Text();
        private final static IntWritable rk = new IntWritable();
        public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
        	Map<String, Double> result = new HashMap<String, Double>();
            //记录已经评分项目
            Map<String, Double> scoreMap = new HashMap<String,Double>();
            if(key.get() == 6){
            	while (values.hasNext()) {
                    String[] str = values.next().toString().split(",");
                    if(str.length > 2){//原始评分矩阵
                    	for(String scoreData : str){
                    		String[] matrix = scoreData.split(":");
                    		scoreMap.put(matrix[0], Double.valueOf(matrix[1]));
                    	}
                    }else {
                        result.put(str[0], Double.parseDouble(str[1]));
                    }
                }
            	System.out.println("scoreMap:"+scoreMap.toString());
            	System.out.println("resultMap:"+result.toString());
            	//过滤已经打分过的项目
                Iterator<String> iter = scoreMap.keySet().iterator();
                while (iter.hasNext()) {
                    String itemID = iter.next();
                    if(result.containsKey(itemID)){
                    	result.remove(itemID);
                    }
                }
                ByValueComparator bvc = new ByValueComparator(result);
                TreeMap<String, Double> sorted_map = new TreeMap<String, Double>(bvc);
                sorted_map.putAll(result);
                for(String k : sorted_map.keySet()){
                   v.set(k+","+result.get(k));
                   rk.set(Integer.parseInt(String.valueOf(key.get()))) ;
  				   output.collect(rk,v);
                }
            }
        }
    }
    
    public static class Step5_FliterAndSortReducerNew extends MapReduceBase implements Reducer<LongWritable, Text, IntWritable, Text> {
        private final static Text v = new Text();
        private final static IntWritable rk = new IntWritable();
        private final static Map<String, Double> result = new HashMap<String, Double>();
        //记录已经评分项目
        private final static Map<String, Double> scoreMap = new HashMap<String,Double>();
        public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
        	while (values.hasNext()) {
        		System.out.println(key.get()+"|"+values.next().toString());
        	}
        	System.out.println("==================================");
//        	String targetUserID = "";
//        	while (values.hasNext()) {
//        		String[] tokens = Recommend.DELIMITER1.split(values.next().toString());
//        		targetUserID = tokens[0];
//        		String value = tokens[1];
//        		if("6".equals(targetUserID)){
//        			String[] str = value.split(",");
//                    if(str.length > 2){//原始评分矩阵
//                    	for(String scoreData : str){
//                    		String[] matrix = scoreData.split(":");
//                    		scoreMap.put(matrix[0], Double.valueOf(matrix[1]));
//                    	}
//                    }else {
//                        result.put(str[0], Double.parseDouble(str[1]));
//                    }
//        		}
//            }
//        	System.out.println("scoreMap:"+scoreMap.toString());
//        	System.out.println("result:"+result.toString());
//        	System.out.println("==================================");
//    		//过滤已经打分过的项目
//            Iterator<String> iter = scoreMap.keySet().iterator();
//            while (iter.hasNext()) {
//                String itemID = iter.next();
//                if(result.containsKey(itemID)){
//                	result.remove(itemID);
//                }
//            }
//            ByValueComparator bvc = new ByValueComparator(result);
//            TreeMap<String, Double> sorted_map = new TreeMap<String, Double>(bvc);
//            sorted_map.putAll(result);
//            for(String k : sorted_map.keySet()){
//               v.set(k+","+result.get(k));
//               rk.set(Integer.parseInt(targetUserID)) ;
//        	   output.collect(rk,v);
//            }
        }
    }
    
    public static class Step5_FliterAndSortMapNew extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
        private final static Text v = new Text();
        private final static IntWritable rk = new IntWritable();
        private final static Map<String, Double> result = new HashMap<String, Double>();
        //记录已经评分项目
        private final static Map<String, Double> scoreMap = new HashMap<String,Double>();
        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String targetUserID = "";
    		System.out.println(value.toString());
            String[] tokens = Recommend.DELIMITER1.split(value.toString());
    		targetUserID = tokens[0];
    		String itemScore = tokens[1];
    		if("6".equals(targetUserID)){
    			String[] str = itemScore.split(",");
                if(str.length > 2){//原始评分矩阵
                	for(String scoreData : str){
                		String[] matrix = scoreData.split(":");
                		scoreMap.put(matrix[0], Double.valueOf(matrix[1]));
                	}
                }else {
                    result.put(str[0], Double.parseDouble(str[1]));
                }
    		}
        	System.out.println("scoreMap:"+scoreMap.toString());
        	System.out.println("result:"+result.toString());
        	System.out.println("==================================");
    		//过滤已经打分过的项目
            Iterator<String> iter = scoreMap.keySet().iterator();
            while (iter.hasNext()) {
                String itemID = iter.next();
                if(result.containsKey(itemID)){
                	result.remove(itemID);
                }
            }
            ByValueComparator bvc = new ByValueComparator(result);
            TreeMap<String, Double> sorted_map = new TreeMap<String, Double>(bvc);
            sorted_map.putAll(result);
            for(String k : sorted_map.keySet()){
               v.set(k+","+result.get(k));
               rk.set(Integer.parseInt(targetUserID)) ;
        	   output.collect(rk,v);
            }
        }
    }
    
    static class ByValueComparator implements Comparator<String> {
		Map<String, Double> base_map;
		public ByValueComparator(Map<String, Double> base_map) {
			this.base_map = base_map;
		}
		public int compare(String arg0, String arg1) {
			if (!base_map.containsKey(arg0) || !base_map.containsKey(arg1)) {
				return 0;
			}
			if (base_map.get(arg0) < base_map.get(arg1)) {
				return 1;
			} else if (base_map.get(arg0) == base_map.get(arg1)) {
				return 0;
			} else {
				return -1;
			}
		}
	}

    public static void run(Map<String, String> path) throws IOException {
        JobConf conf = Recommend.config();

        String input1 = path.get("Step1Output");
        String input2 = path.get("Step4Output");
        String output = path.get("Step5Output");

        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
        hdfs.rmr(output);
        conf.setMapOutputKeyClass(LongWritable.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);
//        conf.setMapperClass(Step5_FliterAndSortMapNew.class);
//        conf.setCombinerClass(Step5_FliterAndSortReducerNew.class);
        conf.setReducerClass(Step5_FliterAndSortReducerNew.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input1), new Path(input2));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()) {
            job.waitForCompletion();
        }
    }

}
