package com.neu.AmazonReviewsAnalysis.Summarization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CountAverageTuple implements Writable {
	
	private float count = 0;
	private float average = 0;
	public float getCount() {
		return count;
	}
	public void setCount(float count) {
		this.count = count;
	}
	public float getAverage() {
		return average;
	}
	public void setAverage(float average) {
		this.average = average;
	}
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeFloat(average);
		out.writeFloat(count);
		
	}
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		average = in.readFloat();
		count = in.readFloat();
	}
	
	public String toString() {
        return String.valueOf(average);
    }
	
}
