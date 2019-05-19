package com.neu.AmazonReviewsAnalysis.TopNProducts;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CountComparator extends WritableComparator {
	
	protected CountComparator() {
		
		super(IntWritable.class,true);
	}
	
	public int compare(WritableComparable w1, WritableComparable w2) {
		IntWritable cw1 = (IntWritable) w1;
		IntWritable cw2 = (IntWritable) w2;
		
		int result = cw1.get() < cw2.get() ? 1 : cw1.get() == cw2.get() ? 0 : -1;
		return result;
	}
}