package com.lorin.hadoop.recommendation.job;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

import com.lorin.hadoop.recommendation.rescorer.JobRescorer;

public class RecommenderPositionResult {

    final static int NEIGHBORHOOD_NUM = 2;
    final static int RECOMMENDER_NUM = 3;

    public static void main(String[] args) throws TasteException, IOException {
        String file = "datafile/job/pv.csv";
        DataModel dataModel = RecommendFactory.buildDataModelNoPref(file);
        RecommenderBuilder rb1 = RecommenderEvaluator.userCityBlock(dataModel);
        RecommenderBuilder rb2 = RecommenderEvaluator.itemLoglikelihood(dataModel);

        long uid = 658;
        System.out.print("userCityBlock    =>");
        filterInvalidDate(uid, rb1, dataModel);
        System.out.print("itemLoglikelihood=>");
        filterInvalidDate(uid, rb2, dataModel);
    }

    public static void filterInvalidDate(long uid, RecommenderBuilder recommenderBuilder, DataModel dataModel) throws TasteException, IOException {
        Set<Long> jobids = getFliterJobID("datafile/job/job.csv",dataModel,uid);
        IDRescorer rescorer = new JobRescorer(jobids);
        List<RecommendedItem> list = recommenderBuilder.buildRecommender(dataModel).recommend(uid, RECOMMENDER_NUM, rescorer);
        RecommendFactory.showItems(uid, list, false);
    }

    public static Set<Long> getFliterJobID(String file,DataModel dataModel,long uid) throws IOException, TasteException {
        BufferedReader br = new BufferedReader(new FileReader(new File(file)));
        Set<Long> jobids = new HashSet<Long>();
        Set<Long> partids = new HashSet<Long>();
        String s = null;
        HashMap<Long,Long> jobSalaryMap = new HashMap<Long,Long>();
        while ((s = br.readLine()) != null) {
	         String[] cols = s.split(",");
	         //保存职位对应的薪资
	       	 jobSalaryMap.put(Long.valueOf(cols[0]), Long.valueOf(cols[2]));
	       	 SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	         Date date = null;
	         try {
	             date = df.parse(cols[1]);
	             if (date.getTime() < df.parse("2013-01-01").getTime()) {//过滤发布过期的工作
	                 jobids.add(Long.parseLong(cols[0]));
	             }else {//记录哪些没过期的工作
	            	 partids.add(Long.parseLong(cols[0]));
	             }
	         } catch (ParseException e) {
	             e.printStackTrace();
	         }
        }
        br.close();
       //计算该用户的浏览过职位的工资的平均工资
        long sumSalary = 0;
        FastIDSet idSet = dataModel.getItemIDsFromUser(uid);
        LongPrimitiveIterator ids = idSet.iterator();
    	while (ids.hasNext()) {
    		long itemId = ids.next();
    		sumSalary += jobSalaryMap.get(itemId);
    	}
    	//预估其期望工资为评价工资的80%
    	double averageSalary = (sumSalary/idSet.size())*0.8;
    	//从那些没过期的职位里面过滤工资低于平均工资的职位
    	Iterator<Long> part = partids.iterator();
        while(part.hasNext()){
        	long itemId = part.next();
        	long salary = jobSalaryMap.get(itemId);
        	if(salary < averageSalary){
        		jobids.add(itemId);
        	}
        }
        return jobids;
    }

}

