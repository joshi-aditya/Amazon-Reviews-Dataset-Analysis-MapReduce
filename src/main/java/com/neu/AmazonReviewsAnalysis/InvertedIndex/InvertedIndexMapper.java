package com.neu.AmazonReviewsAnalysis.InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private Text productId = new Text();
	private Text userId = new Text();
	
	public void map(LongWritable key, Text values, Context context) throws InterruptedException{
		
		if(key.get()==0){
			return;
		}
		
		try{
			String[] tokens = values.toString().split("\\t");
			userId.set(tokens[1]);
			productId.set(tokens[3]);
			context.write(productId, userId);
		}
		catch(IOException  ex){
			System.out.println("Error in Mapper" + ex.getMessage());
		}
	}
}