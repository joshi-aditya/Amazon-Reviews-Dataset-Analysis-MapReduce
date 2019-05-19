package com.neu.AmazonReviewsAnalysis.Summarization;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageMapper extends Mapper<LongWritable, Text, Text, CountAverageTuple> {

	private Text text = new Text();
	private CountAverageTuple outCountAverage = new CountAverageTuple();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		if(key.get()==0){
			return;
		}

		else{

			String[] line = value.toString().split("\\t");
			String productId = line[3].trim();
			text.set(productId);
			outCountAverage.setCount(1);
			outCountAverage.setAverage(Float.valueOf(line[7].trim()));

			context.write(text, outCountAverage);

		}
	}
}