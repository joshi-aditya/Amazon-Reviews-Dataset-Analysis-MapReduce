package com.neu.AmazonReviewsAnalysis.SplitByRating;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class BinningMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	
	private MultipleOutputs<Text, NullWritable> output = null;
	
	@Override
	protected void setup(Context context){
		output = new MultipleOutputs(context);
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		if(key.get()==0){
			return;
		}
		
		String[] token = value.toString().split("\\t");
		String rating = token[7].trim();
		
		if(rating.equals("1")){
			output.write("bins", value, NullWritable.get(), "Rating 1");
		}
		if(rating.equals("2")){
			output.write("bins", value, NullWritable.get(), "Rating 2");
		}
		if(rating.equals("3")){
			output.write("bins", value, NullWritable.get(), "Rating 3");
		}
		if(rating.equals("4")){
			output.write("bins", value, NullWritable.get(), "Rating 4");
		}
		if(rating.equals("5")){
			output.write("bins", value, NullWritable.get(), "Rating 5");
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		output.close();
	}
}

