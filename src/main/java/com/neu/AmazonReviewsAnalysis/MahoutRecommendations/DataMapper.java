package com.neu.AmazonReviewsAnalysis.MahoutRecommendations;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DataMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		if(key.get()==0){
			return;
		}

		else{

			String[] line = value.toString().split("\\t");
			
			Text res = new Text();
			res.set(line[1] + "," + line[4] + "," + line[7]);

			context.write(NullWritable.get(), res);

		}
	}
}