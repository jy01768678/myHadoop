package com.lorin.hadoop.recommendation.job;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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

public class RecommenderFilterSalaryResult {
	final static int NEIGHBORHOOD_NUM = 2;
    final static int RECOMMENDER_NUM = 3;
   
	public static void main(String[] args) throws TasteException, IOException{
		String file = "datafile/job/pv.csv";
        DataModel dataModel = RecommendFactory.buildDataModelNoPref(file);
        RecommenderBuilder rb1 = RecommenderEvaluator.userCityBlock(dataModel);
        RecommenderBuilder rb2 = RecommenderEvaluator.itemLoglikelihood(dataModel);

        LongPrimitiveIterator iter = dataModel.getUserIDs();
        while (iter.hasNext()) {
            long uid = iter.nextLong();
            System.out.print("userCityBlock    =>");
            filterSalary(uid, rb1, dataModel);
            System.out.print("itemLoglikelihood=>");
            filterSalary(uid, rb2, dataModel);
        }
	}

	private static void filterSalary(long uid, RecommenderBuilder recommenderBuilder,
			DataModel dataModel) throws TasteException, IOException{
		Set<Long> jobids = getLowAverageSalayJobID("datafile/job/job.csv",dataModel,uid);
        IDRescorer rescorer = new JobRescorer(jobids);
        List<RecommendedItem> list = recommenderBuilder.buildRecommender(dataModel).recommend(uid, RECOMMENDER_NUM, rescorer);
        RecommendFactory.showItems(uid, list, false);
		
	}

	private static Set<Long> getLowAverageSalayJobID(String file,DataModel dataModel,long uid) throws IOException, TasteException{
		 BufferedReader br = new BufferedReader(new FileReader(new File(file)));
        Set<Long> jobids = new HashSet<Long>();
        String s = null;
        HashMap<Long,Long> jobSalaryMap = new HashMap<Long,Long>();
        while ((s = br.readLine()) != null) {
        	 String[] cols = s.split(",");
        	 jobSalaryMap.put(Long.valueOf(cols[0]), Long.valueOf(cols[2]));
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
        //过滤公司小于平均工资的itemId
        Iterator<Long> jobSalary =jobSalaryMap.keySet().iterator();
        while(jobSalary.hasNext()){
        	long itemId = (Long)jobSalary.next();
        	double salary = jobSalaryMap.get(itemId);
        	if(salary < averageSalary){
        		jobids.add(itemId);
        	}
        }
		return jobids;
	}
}
