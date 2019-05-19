package com.neu.AmazonReviewsAnalysis.Join;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
    		throws IOException, InterruptedException {
    	
    	Set<String> listA = new HashSet<String>();
        Set<String> listB = new HashSet<String>();
        for (Text text: values) {
            if (text.toString().startsWith("#"))
                listA.add(text.toString().substring(1));
            else if (text.toString().startsWith("*"))
                listB.add(text.toString().substring(1));
        }
        
        if(!listA.isEmpty() && !listB.isEmpty()) {
	        for (String A: listA) {
	            for (String B: listB) {
	                context.write(new Text(A), new Text(B));
	            }
	        }
        }
    }
}
