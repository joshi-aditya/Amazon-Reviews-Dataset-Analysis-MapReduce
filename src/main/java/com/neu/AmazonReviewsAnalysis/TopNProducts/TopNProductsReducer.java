package com.neu.AmazonReviewsAnalysis.TopNProducts;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNProductsReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
	
	int count = 0;
	
	// default value = 10
	private int N = 10;
	
	@Override
	public void reduce(IntWritable key, Iterable<Text> value,Context context)
			throws IOException, InterruptedException{
		
		for(Text val: value){
			if(count<N)
			{
				context.write(key,val);
			}
			count++;
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// default = 10
		this.N = context.getConfiguration().getInt("N", 10); 
	}
}