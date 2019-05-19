package com.neu.AmazonReviewsAnalysis.Summarization;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AggregateMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

	private static final IntWritable one = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		if(key.get()==0){
			return;
			
		} else {
		
			String[] line = value.toString().split("\\t");
			String productId = line[3].trim();
			
			context.write(new Text(productId), one);
		
		}
	}

}
