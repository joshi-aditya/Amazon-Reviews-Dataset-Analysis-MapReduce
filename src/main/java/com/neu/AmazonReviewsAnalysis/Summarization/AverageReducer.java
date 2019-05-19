package com.neu.AmazonReviewsAnalysis.Summarization;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, CountAverageTuple, Text, CountAverageTuple>{

	private CountAverageTuple result = new CountAverageTuple();
	
	@Override
	protected void reduce(Text key, Iterable<CountAverageTuple> values, Context context) throws IOException, InterruptedException{

		float sum = 0;
		float count = 0;
		
		for (CountAverageTuple val : values) {
            sum += val.getCount() * val.getAverage();
            count += val.getCount();
        }
		result.setCount(count);
        
        float scale = (float) Math.pow(10, 2);
        result.setAverage(Math.round((sum/count) * scale) / scale);
        
		context.write(key,result);
	}

}