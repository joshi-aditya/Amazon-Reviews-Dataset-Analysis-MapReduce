package com.neu.AmazonReviewsAnalysis.MahoutRecommendations;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class RecommendationMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private Text text = new Text();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{


		String[] line = value.toString().split(",");
		String userId = line[0].trim();
		text.set(userId);

		context.write(text, NullWritable.get());
	}
}