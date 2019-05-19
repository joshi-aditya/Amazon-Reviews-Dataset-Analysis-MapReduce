package com.neu.AmazonReviewsAnalysis.Join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class RatingsMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		String[] line = value.toString().split("\\t");
		
		Text productId = new Text();
		Text rating = new Text();
		productId.set(line[0].trim());
		rating.set("*" + line[1].trim());

		context.write(productId, rating);
	}
}