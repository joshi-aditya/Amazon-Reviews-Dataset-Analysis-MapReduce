package com.neu.AmazonReviewsAnalysis.MahoutRecommendations;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class ProductRecommendation {

	public static void Recommend(Path inputPath) throws IOException {

		try {
			String path = inputPath.toString();
			String fname = "/part-r-00000";
			path = path + fname;
			File userPreferencesFile = new File(path);

			DataModel dataModel = new FileDataModel(userPreferencesFile);

			UserSimilarity userSimilarity = new PearsonCorrelationSimilarity(dataModel);

			UserNeighborhood userNeighborhood = new NearestNUserNeighborhood(5, userSimilarity, dataModel);
			// Create a generic user based recommender with the dataModel, the userNeighborhood and the userSimilarity
			Recommender genericRecommender = new GenericUserBasedRecommender(dataModel,
					userNeighborhood, userSimilarity);
		
	            
			for (LongPrimitiveIterator iterator = dataModel.getUserIDs(); iterator.hasNext();) {
				long userId = iterator.nextLong();
				
				// 3 recommendations for each user
				List<RecommendedItem> itemRecommendations = genericRecommender.recommend(userId, 3);
				
				if (!itemRecommendations.isEmpty()) {
					System.out.format("User Id: %d%n", userId);
					for (RecommendedItem recommendedItem : itemRecommendations) {
						System.out.format(" Recommened Item Id: %d. Strength of preference: %f%n",
								recommendedItem.getItemID(), recommendedItem.getValue());
					}
				}
			}

		}  catch (IOException ex) {
			
			System.out.println("Exception: " + ex.getMessage());
		
		}	catch (TasteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}	
}
