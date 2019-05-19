package com.neu.AmazonReviewsAnalysis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.neu.AmazonReviewsAnalysis.InvertedIndex.InvertedIndexMapper;
import com.neu.AmazonReviewsAnalysis.InvertedIndex.InvertedIndexReducer;
import com.neu.AmazonReviewsAnalysis.Join.JoinReducer;
import com.neu.AmazonReviewsAnalysis.Join.RatingsMapper;
import com.neu.AmazonReviewsAnalysis.Join.TopProductsMapper;
import com.neu.AmazonReviewsAnalysis.MahoutRecommendations.DataMapper;
import com.neu.AmazonReviewsAnalysis.MahoutRecommendations.RecommendationMapper;
import com.neu.AmazonReviewsAnalysis.MahoutRecommendations.RecommendationReducer;
import com.neu.AmazonReviewsAnalysis.SplitByRating.BinningMapper;
import com.neu.AmazonReviewsAnalysis.Summarization.AggregateMapper;
import com.neu.AmazonReviewsAnalysis.Summarization.AggregateReducer;
import com.neu.AmazonReviewsAnalysis.Summarization.AverageMapper;
import com.neu.AmazonReviewsAnalysis.Summarization.AverageReducer;
import com.neu.AmazonReviewsAnalysis.Summarization.CountAverageTuple;
import com.neu.AmazonReviewsAnalysis.TopNProducts.CountComparator;
import com.neu.AmazonReviewsAnalysis.TopNProducts.TopNProductsMapper;
import com.neu.AmazonReviewsAnalysis.TopNProducts.TopNProductsReducer;


/**
 * @author aditya
 *
 */
public class Driver  {

	private static final Log logger = LogFactory.getLog(Driver.class);

	public static void main( String[] args ) {
		
		if (args.length != 9) {
            logger.error("Usage: com.neu.AmazonReviewsAnalysis.Driver path arguments missing");
            System.exit(1);
        }

		try {
			Configuration configuration = new Configuration();
			FileSystem fs = FileSystem.get(configuration);

			// Input/Output Path for MapReduce Jobs
			Path inputPath = new Path(args[0]);
			Path summarizationOutputPath = new Path(args[1]);
			Path aggregationOutputPath = new Path(args[2]);
			Path topNProductsOutputPath = new Path(args[3]);
			Path joinOutputPath = new Path(args[4]);
			Path binningOutputPath = new Path(args[5]);
			Path invertedIndexOutputPath = new Path(args[6]);
			Path dataCleanOutputPath = new Path(args[7]);
			Path recommedationOutputPath = new Path(args[8]);

			// Define MapReduce Job
			Job summarizationJob = Job.getInstance(configuration, "Product Ratings Average");
			summarizationJob.setJarByClass(Driver.class);

			// Set Input and Output locations for summarizationJob Job
			FileInputFormat.setInputPaths(summarizationJob, inputPath);
			FileOutputFormat.setOutputPath(summarizationJob, summarizationOutputPath);

			// Set Input and Output formats for summarizationJob Job
			summarizationJob.setInputFormatClass(TextInputFormat.class);
			summarizationJob.setOutputFormatClass(TextOutputFormat.class);

			// Set Mapper/Combiner/Reducer classes for MeanStdHelpfulReviews Job
			summarizationJob.setMapperClass(AverageMapper.class);
			summarizationJob.setReducerClass(AverageReducer.class);
			summarizationJob.setCombinerClass(AverageReducer.class);

			// Set key/values classes
			summarizationJob.setOutputKeyClass(Text.class);
			summarizationJob.setOutputValueClass(CountAverageTuple.class);

			// Check if the output path is available or not
			if (fs.exists(summarizationOutputPath)) {
				fs.delete(summarizationOutputPath, true);
			}
			boolean issummarizationJobSuccessful = (summarizationJob.waitForCompletion(true));
			
			boolean isAggregationJobSuccesful = false;
			if (issummarizationJobSuccessful) {

				Job aggregationJob = Job.getInstance(configuration, "Review count");
				aggregationJob.setJarByClass(Driver.class);

				aggregationJob.setMapperClass(AggregateMapper.class);
				aggregationJob.setCombinerClass(AggregateReducer.class);
				aggregationJob.setReducerClass(AggregateReducer.class);

				aggregationJob.setInputFormatClass(TextInputFormat.class);
				aggregationJob.setOutputFormatClass(TextOutputFormat.class);
				
				aggregationJob.setOutputKeyClass(Text.class);
				aggregationJob.setOutputValueClass(IntWritable.class);
				
				FileInputFormat.addInputPath(aggregationJob, inputPath);
				FileOutputFormat.setOutputPath(aggregationJob, aggregationOutputPath);
				
				if (fs.exists(aggregationOutputPath)) {
					fs.delete(aggregationOutputPath, true);
				}
				
				isAggregationJobSuccesful = aggregationJob.waitForCompletion(true);

			}
			
			boolean topNproductsJobSuccessful = false;
			if (issummarizationJobSuccessful) {

				Job topNProductsJob = Job.getInstance(configuration, "Top N Rated Products");
				topNProductsJob.setJarByClass(Driver.class);

				int N = 200;
				topNProductsJob.getConfiguration().setInt("N", N);

				topNProductsJob.setInputFormatClass(TextInputFormat.class);
				topNProductsJob.setOutputFormatClass(TextOutputFormat.class);

				topNProductsJob.setMapperClass(TopNProductsMapper.class);
				topNProductsJob.setSortComparatorClass(CountComparator.class);
				topNProductsJob.setReducerClass(TopNProductsReducer.class);
				topNProductsJob.setNumReduceTasks(1);

				topNProductsJob.setMapOutputKeyClass(IntWritable.class);   
				topNProductsJob.setMapOutputValueClass(Text.class);   

				topNProductsJob.setOutputKeyClass(IntWritable.class);
				topNProductsJob.setOutputValueClass(Text.class);
				
				FileInputFormat.setInputPaths(topNProductsJob, aggregationOutputPath);
				FileOutputFormat.setOutputPath(topNProductsJob, topNProductsOutputPath);

				if (fs.exists(topNProductsOutputPath)) {
					fs.delete(topNProductsOutputPath, true);
				}
				
				topNproductsJobSuccessful = topNProductsJob.waitForCompletion(true);

			}
			
			boolean joinJobSuccesful = false;
			if (topNproductsJobSuccessful) {

				Job joinsJob = Job.getInstance(configuration, "Join");
				joinsJob.setJarByClass(Driver.class);
				
				MultipleInputs.addInputPath(joinsJob, topNProductsOutputPath, 
						TextInputFormat.class, TopProductsMapper.class);
		        MultipleInputs.addInputPath(joinsJob, summarizationOutputPath, 
		        		TextInputFormat.class, RatingsMapper.class);
		        FileOutputFormat.setOutputPath(joinsJob, joinOutputPath);
		        
				joinsJob.setReducerClass(JoinReducer.class);
	
				joinsJob.setMapOutputKeyClass(Text.class);   
				joinsJob.setMapOutputValueClass(Text.class);   
				  
				joinsJob.setOutputKeyClass(Text.class);
				joinsJob.setOutputValueClass(Text.class);

				if (fs.exists(joinOutputPath)) {
					fs.delete(joinOutputPath, true);
				}
				
				joinJobSuccesful = joinsJob.waitForCompletion(true);

			}
			
			boolean binningJobSuccesful = false;
			if(joinJobSuccesful) {
				
				Job binningJob = Job.getInstance(configuration, "Binning");
				binningJob.setJarByClass(Driver.class);
				
				binningJob.setMapperClass(BinningMapper.class);
				binningJob.setMapOutputKeyClass(Text.class);
				binningJob.setMapOutputValueClass(NullWritable.class);
				
				//No combiner, partitioner or reducer is used in this pattern!
				binningJob.setNumReduceTasks(1);
				
				FileInputFormat.setInputPaths(binningJob, inputPath);
				FileOutputFormat.setOutputPath(binningJob, binningOutputPath);
				
				if (fs.exists(binningOutputPath)) {
					fs.delete(binningOutputPath, true);
				}
				
				MultipleOutputs.addNamedOutput(binningJob, "bins", TextOutputFormat.class, 
						Text.class, NullWritable.class);
				MultipleOutputs.setCountersEnabled(binningJob, true);
				
				binningJobSuccesful = binningJob.waitForCompletion(true);
			}
			
			boolean invertedIndexJobSuccesful = false;
			if (binningJobSuccesful) {

				Job invertedIndexJob = Job.getInstance(configuration, "Inverted Index");
				invertedIndexJob.setJarByClass(Driver.class);

				invertedIndexJob.setMapperClass(InvertedIndexMapper.class);
				invertedIndexJob.setReducerClass(InvertedIndexReducer.class);

				invertedIndexJob.setInputFormatClass(TextInputFormat.class);
				invertedIndexJob.setOutputFormatClass(TextOutputFormat.class);
				
				invertedIndexJob.setMapOutputKeyClass(Text.class);
				invertedIndexJob.setMapOutputValueClass(Text.class);
				invertedIndexJob.setOutputKeyClass(Text.class);
				invertedIndexJob.setOutputValueClass(Text.class);
				
				FileInputFormat.addInputPath(invertedIndexJob, inputPath);
				FileOutputFormat.setOutputPath(invertedIndexJob, invertedIndexOutputPath);
				
				if (fs.exists(invertedIndexOutputPath)) {
					fs.delete(invertedIndexOutputPath, true);
				}
				
				invertedIndexJobSuccesful = invertedIndexJob.waitForCompletion(true);

			}
			
			boolean mahoutDataCleanJobSuccesful = false;
			if(invertedIndexJobSuccesful) {
				
				Job dataCleanJob = Job.getInstance(configuration, "Data clean");
				dataCleanJob.setJarByClass(Driver.class);
				
				dataCleanJob.setMapperClass(DataMapper.class);
				dataCleanJob.setMapOutputKeyClass(NullWritable.class);
				dataCleanJob.setMapOutputValueClass(Text.class);
				dataCleanJob.setNumReduceTasks(1);
				
				FileInputFormat.setInputPaths(dataCleanJob, inputPath);
				FileOutputFormat.setOutputPath(dataCleanJob, dataCleanOutputPath);
				
				if (fs.exists(dataCleanOutputPath)) {
					fs.delete(dataCleanOutputPath, true);
				}
				
				mahoutDataCleanJobSuccesful = dataCleanJob.waitForCompletion(true);
			}
			
			boolean recommendationJobSuccesful = false;
			if(mahoutDataCleanJobSuccesful) {
				
				Job recommendationJob = Job.getInstance(configuration, "Recommendation");
				String path = dataCleanOutputPath.toString();
				recommendationJob.getConfiguration().set("DataPath", path);
				
				recommendationJob.setJarByClass(Driver.class);
				FileInputFormat.setInputPaths(recommendationJob, dataCleanOutputPath);
				FileOutputFormat.setOutputPath(recommendationJob, recommedationOutputPath);
				
				recommendationJob.setMapperClass(RecommendationMapper.class);
				recommendationJob.setReducerClass(RecommendationReducer.class);
				recommendationJob.setNumReduceTasks(1);
				
				recommendationJob.setMapOutputKeyClass(Text.class);
				recommendationJob.setMapOutputValueClass(NullWritable.class);
				
				recommendationJob.setOutputKeyClass(NullWritable.class);
				recommendationJob.setOutputValueClass(Text.class);
				
				if(fs.exists(recommedationOutputPath)) {
					fs.delete(recommedationOutputPath, true);
				}
				
				recommendationJobSuccesful = recommendationJob.waitForCompletion(true);
				//ProductRecommendation.Recommend(dataCleanOutputPath);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
