package com.lorin.hadoop;

import java.util.TreeMap;
import java.util.regex.Pattern;

import com.lorin.hadoop.recommend.Recommend;

/**
 * Hello world!
 * 
 */
public class App {
	public static final Pattern DELIMITER = Pattern.compile("[\t,]");
	public static void main(String[] args) {
		String value = "1	102:3.0,103:2.5,101:5.0";
		String[] tokens = Recommend.DELIMITER.split(value);
		for(String token:tokens){
			System.out.println(token);
		}
		TreeMap<Integer,String> map = new TreeMap<Integer,String>();
		map.put(2, "2");
		map.put(1, "1");
		System.out.println(map.toString());
	}
}
