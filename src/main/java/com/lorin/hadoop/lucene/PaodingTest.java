package com.lorin.hadoop.lucene;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;




import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import net.paoding.analysis.analyzer.PaodingAnalyzer;

public class PaodingTest {

	public static void main(String[] args) throws IOException {
		FileReader fr=new FileReader("F:/ltool/digital/camera/camera3");
        BufferedReader br=new BufferedReader(fr);
        PaodingAnalyzer analyzer = new PaodingAnalyzer();
        int i=0;
        try {
        	TokenStream ts = analyzer.tokenStream("", br);
        	while(ts.incrementToken()){
        		System.out.print(ts.getAttribute(CharTermAttribute.class).toString() + "\t");
        	}
		}finally{
			try {
				br.close();
				fr.close();
				analyzer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
