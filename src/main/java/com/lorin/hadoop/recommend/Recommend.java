package com.lorin.hadoop.recommend;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;

public class Recommend {

    public static final String HDFS = "hdfs://192.168.1.102:9000";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");
    public static final Pattern DELIMITER1 = Pattern.compile("[\t]");
    public static void main(String[] args) throws Exception {
        Map<String, String> path = new HashMap<String, String>();
//        path.put("data", "logfile/small2.csv");
        path.put("Step1Input", HDFS + "/usr/hdfs/recommend");
        path.put("Step1Output", path.get("Step1Input") + "/step1");
//        path.put("Step2Input", path.get("Step1Output"));
//        path.put("Step2Output", path.get("Step1Input") + "/step2");
        //输入第一步求出的用户评分矩阵
//        path.put("Step3Input1", path.get("Step1Output"));
//        path.put("Step3Output1", path.get("Step1Input") + "/step3_1");
        //输入第二部求出的项目同现矩阵
//        path.put("Step3Input2", path.get("Step2Output"));
//        path.put("Step3Output2", path.get("Step1Input") + "/step3_2");
        
        path.put("Step4Input1", path.get("Step3Output1"));
        path.put("Step4Input2", path.get("Step3Output2"));
        path.put("Step4Output", path.get("Step1Input") + "/step4");
        
        path.put("Step5Output", path.get("Step1Input") + "/step5");

//        Step1.run(path);
//        Step2.run(path);
//        Step3.run1(path);
//        Step3.run2(path);
//        Step4.run(path);
        Step5.run(path);
        System.exit(0);
    }

    public static JobConf config() {
        JobConf conf = new JobConf(Recommend.class);
        conf.setJobName("Recommand");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
//        conf.set("io.sort.mb", "2048");
        return conf;
    }

}
