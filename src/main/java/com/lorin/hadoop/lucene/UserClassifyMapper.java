package com.lorin.hadoop.lucene;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.classifier.ClassifierResult;
import org.apache.mahout.classifier.bayes.Algorithm;
import org.apache.mahout.classifier.bayes.BayesAlgorithm;
import org.apache.mahout.classifier.bayes.BayesParameters;
import org.apache.mahout.classifier.bayes.CBayesAlgorithm;
import org.apache.mahout.classifier.bayes.ClassifierContext;
import org.apache.mahout.classifier.bayes.Datastore;
import org.apache.mahout.classifier.bayes.InMemoryBayesDatastore;
import org.apache.mahout.classifier.bayes.InvalidDatastoreException;
import org.apache.mahout.common.nlp.NGrams;

public class UserClassifyMapper extends Mapper<Text, Text, Text, IntWritable>{

	private Text outKey = new Text();
	private static final IntWritable ONE = new IntWritable(1);
	
	private int gramSize = 1;
	private ClassifierContext classifier;
	private String defaultCategory;
	
	
	public void map(Text key, Text value,Context context)
			throws IOException, InterruptedException {
		String docLabel = "";
		String userID = key.toString();
		List<String> ngrams = new NGrams(value.toString(),gramSize).generateNGramsWithoutLabel();
		
		try{
			ClassifierResult result = null;
			result = classifier.classifyDocument(ngrams.toArray(new String[ngrams.size()]),defaultCategory);
			docLabel = result.getLabel();
		}catch(InvalidDatastoreException e){
			e.printStackTrace();
		}
		outKey.set(userID+ "|" +docLabel);
		context.write(outKey, ONE);
	}

	public void setup(Context context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		BayesParameters bayes = new BayesParameters(conf.get("bayes.parameters",""));
		
		Algorithm algorithm = null;
		Datastore dataStore = null;
		if("bayes".equalsIgnoreCase(bayes.get("classifierType"))){
			algorithm = new BayesAlgorithm();
			dataStore = new InMemoryBayesDatastore(bayes);
		} else if("cbayes".equalsIgnoreCase(bayes.get("classifierType"))){
			algorithm = new CBayesAlgorithm();
			dataStore = new InMemoryBayesDatastore(bayes);
		}
		
		classifier = new ClassifierContext(algorithm,dataStore);
		try{
			classifier.initialize(); 
		}catch (InvalidDatastoreException e){
			e.printStackTrace();
		}
		
		defaultCategory = bayes.get("defaultCat");
		gramSize = bayes.getGramSize();
	}

}
