package com.lorin.hadoop.mr.kpi;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DomainText {

	public static void main(String[] args) {
		String url = "http://f.angularjs.com.cn/A00n";
		Pattern p =Pattern.compile("[^//]*?\\.(com|cn|net|org|biz|info|cc|tv)", Pattern.CASE_INSENSITIVE);
		Pattern p1 =Pattern.compile("(^[https|http]://)?([a-z\\.-]+)\\.([a-z\\.]+)\\.([com|cn|net|org|biz|info|cc|tv]+)", Pattern.CASE_INSENSITIVE);
		Matcher matcher = p1.matcher(url);  
        if(matcher.find()){
        	System.out.println(matcher.group()); 
        	System.out.println(matcher.group(0)); 
        	System.out.println(matcher.group(1)); 
        	System.out.println(matcher.group(2)); 
        	System.out.println(matcher.group(3)); 
        	System.out.println(matcher.group(4)); 
        }
	}
}
