package com.neu.AmazonReviewsAnalysis.TopNProducts;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNProductsMapper extends Mapper<LongWritable, Text, IntWritable, Text>
{
	public void map(LongWritable key, Text value,Context context){
		
		String[] row = value.toString().split("\\t");
		
		String productId = row[0].trim();
		int count = Integer.parseInt(row[1].trim());
		
		try{
			
			Text id = new Text(productId);
			IntWritable prodRating = new IntWritable(count);
			context.write(prodRating, id);
			
		}catch(Exception e){
			
		}
	}
}