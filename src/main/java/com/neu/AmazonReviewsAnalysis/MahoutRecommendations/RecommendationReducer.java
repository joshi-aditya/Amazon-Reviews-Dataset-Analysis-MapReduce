package com.neu.AmazonReviewsAnalysis.MahoutRecommendations;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class RecommendationReducer extends Reducer<Text, NullWritable, NullWritable, Text>{
	
	private String path = new String();
	private File userPreferencesFile;
	private DataModel dataModel;
	private UserSimilarity userSimilarity;
	private UserNeighborhood userNeighborhood;
	private Recommender genericRecommender;
		
	@Override
	protected void reduce(Text key, Iterable<NullWritable> value, Context context) 
			throws IOException,InterruptedException, FileNotFoundException{

		try {
			
			Long userId = Long.valueOf(key.toString());
			List<RecommendedItem> recs = genericRecommender.recommend(userId,2);
            
            if (!recs.isEmpty()) {
				
				Text res = new Text();
				for (RecommendedItem recommendedItem : recs) {
					
					res.set(key.toString() + "Recommened Item Id: " + recommendedItem.getItemID() +
							" Strength of preference: " + recommendedItem.getValue());
				}
				context.write(NullWritable.get(), res);
			}
            			
		}	catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}
	
	@Override
	protected void setup(Context context) 
			throws IOException, InterruptedException, FileNotFoundException {
		
		try {
			this.path = context.getConfiguration().get("DataPath");
			String fname = "/part-r-00000";
			this.path = this.path + fname;

			this.userPreferencesFile = new File(path);
	
			this.dataModel = new FileDataModel(this.userPreferencesFile);
	
			this.userSimilarity = new PearsonCorrelationSimilarity(this.dataModel);
	
			this.userNeighborhood = new NearestNUserNeighborhood(5, this.userSimilarity, this.dataModel);
			
			// Create a generic user based recommender with the dataModel, the userNeighborhood and the userSimilarity
			this.genericRecommender = new GenericUserBasedRecommender(this.dataModel,
					this.userNeighborhood, this.userSimilarity);
			
		} catch (FileNotFoundException ex) {
			
			System.out.println("Exception: " + ex.getMessage());
		}	catch (TasteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}