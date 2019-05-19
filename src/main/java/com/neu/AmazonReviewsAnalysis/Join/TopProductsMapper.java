package com.neu.AmazonReviewsAnalysis.Join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TopProductsMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		String[] line = value.toString().split("\\t");
		
		Text productId = new Text();
		Text count = new Text();
		productId.set(line[1].trim());
		count.set("#"+ line[1] + " "+ line[0].trim());

		context.write(productId, count);
	}
}