package es.upm.multidimensional;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.RefreshHelper;
import org.apache.mahout.cf.taste.impl.recommender.EstimatedPreferenceCapper;
import org.apache.mahout.cf.taste.impl.recommender.TopItems;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Rescorer;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.common.LongPair;
import org.slf4j.Logger;

public class MultidimensionalRecommender  implements UserBasedRecommender{
	
	private Logger log;
	private final RefreshHelper refreshHelper;
	private HashMap<String, DataModel> dataModels;
	private HashMap<String, UserSimilarity> similarities;
	private HashMap<String, UserNeighborhood> neighborhoods;
	private HashMap<String, Double> ponderations;
	private HashMap<String,EstimatedPreferenceCapper> cappers;
	private String[] dimensions;

	
	public MultidimensionalRecommender(HashMap<String, DataModel> dataModels,HashMap<String, 
								UserSimilarity> similarities,HashMap<String,
								UserNeighborhood> neighborhoods, HashMap<String, Double> ponderations, 
								String[] dimensions,Logger log) throws Exception{
		this.log=log;
		this.dataModels=dataModels;
		this.similarities=similarities;
		this.neighborhoods=neighborhoods;
		this.ponderations=ponderations;
		this.dimensions=dimensions;
	
		this.refreshHelper = new RefreshHelper(null);
		this.cappers=new HashMap<String,EstimatedPreferenceCapper>();
		for(int i=0; i<dimensions.length; i++){
			try{
				refreshHelper.addDependency(dataModels.get(dimensions[i]));
				refreshHelper.addDependency(similarities.get(dimensions[i]));
				refreshHelper.addDependency(neighborhoods.get(dimensions[i]));
				cappers.put(dimensions[i], buildCapper(dimensions[i]));
			}catch(Exception e){
				throw new Exception("For some reasons dependencies cannot be processed.");
			}			
		}
		log.info("Recommender instance created sucessfully.");
	}
	
	  // ¡OJO!, solo referencia a la primera dimensión.
	@Override
	public DataModel getDataModel(){
		return dataModels.get(dimensions[0]);
	}
	
	@Override
	  public List<RecommendedItem> recommend(long userID, int howMany, IDRescorer rescorer) throws TasteException {
	    if (howMany < 1) {
	      throw new IllegalArgumentException("howMany must be at least 1");
	    }
	    
	    log.info("Recommending items for user ID '{}'", userID);
	    
	    HashMap<String, long[]> theNeighborhoods = new HashMap<String, long[]>();
	    
	    for (int i=0; i<dimensions.length; i++){
	    	long[] theNeighborhood = neighborhoods.get(dimensions[i]).getUserNeighborhood(userID);
	    	if (theNeighborhood.length==0){
	  	      	log.info("The user with ID "+userID+" has no neighbors.");
	  	      	return Collections.emptyList();
	    	}	    		
	    	theNeighborhoods.put(dimensions[i], theNeighborhood);
	    }

	    FastIDSet allItemIDs = new FastIDSet();
	    for(int i=0; i<dimensions.length; i++){
	    	FastIDSet aux = getAllOtherItems(theNeighborhoods.get(dimensions[i]), userID, dimensions[i]);
	    	allItemIDs.addAll(aux);
	    }
	    	    
	    //TopItems.Estimator<Long> estimator = new Estimator(userID, theNeighborhoodLiked, theNeighborhoodRead);
	    TopItems.Estimator<Long> estimator = new Estimator(userID, theNeighborhoods);
	    List<RecommendedItem> topItems = TopItems
	        .getTopItems(howMany, allItemIDs.iterator(), rescorer, estimator);
	    
	    log.info("Recommendations are: {}", topItems);
	    return topItems;
	  }
	
	@Override
	public List<RecommendedItem> recommend(long userID, int howMany) throws TasteException {
		return this.recommend(userID, howMany, null);
	}
	
	  @Override
	  public void refresh(Collection<Refreshable> alreadyRefreshed) {
	    refreshHelper.refresh(alreadyRefreshed);
	    for (int i=0; i<dimensions.length; i++)
	    	cappers.put(dimensions[i], buildCapper(dimensions[i]));	    
	  }
	  
	  @Override
	  public String toString() {
	    return "Multidimensional recommender.";
	  }
	  
	  // ¡OJO!, solo referencia a la primera dimensión.
	  @Override
	  public float estimatePreference(long userID, long itemID) throws TasteException {
	    DataModel model = getDataModel();
	    Float actualPref = model.getPreferenceValue(userID, itemID);
	    if (actualPref != null) {
	      return actualPref;
	    }
	    long[] theNeighborhood = neighborhoods.get(dimensions[0]).getUserNeighborhood(userID);
	    return doEstimatePreference(userID, theNeighborhood, itemID, dimensions[0]);
	  }
	  
	  // ¡OJO!, solo referencia a la primera dimensión.
	  @Override
	  public void setPreference(long userID, long itemID, float value) throws TasteException {
	    if (Double.isNaN(value)) {
	      throw new IllegalArgumentException("Invalid value: " + value);
	    }
	    log.debug("Setting preference for user {}, item {}", userID, itemID);
	    dataModels.get(dimensions[0]).setPreference(userID, itemID, value);	    
	  }
	  
	  @Override
	  public void removePreference(long userID, long itemID) throws TasteException {
	    log.debug("Remove preference for user '{}', item '{}'", userID, itemID);
	    for(int i=0; i<dimensions.length; i++)
	    	dataModels.get(dimensions[i]).removePreference(userID, itemID);
	   }
	  
	  @Override
	  public long[] mostSimilarUserIDs(long userID, int howMany) throws TasteException {
	    return mostSimilarUserIDs(userID, howMany, null);
	  }
	  
	  @Override
	  public long[] mostSimilarUserIDs(long userID, int howMany, Rescorer<LongPair> rescorer) throws TasteException {
	    TopItems.Estimator<Long> estimator = new MostSimilarEstimator(userID, similarities, rescorer);
	    return doMostSimilarUsers(howMany, estimator);
	  }
	  
	  private long[] doMostSimilarUsers(int howMany, TopItems.Estimator<Long> estimator) throws TasteException {
		    DataModel model = getDataModel();
		    return TopItems.getTopUsers(howMany, model.getUserIDs(), null, estimator);
		  }
	
	protected FastIDSet getAllOtherItems(long[] theNeighborhood, long theUserID, String dimension) throws TasteException {
	    DataModel dataModel = dataModels.get(dimension);
	    FastIDSet possibleItemIDs = new FastIDSet();
	    for (long userID : theNeighborhood) {
	      possibleItemIDs.addAll(dataModel.getItemIDsFromUser(userID));
	    }
	    possibleItemIDs.removeAll(dataModel.getItemIDsFromUser(theUserID));
	    return possibleItemIDs;
	}
	
	  private EstimatedPreferenceCapper buildCapper(String dimension) {
		    DataModel dataModel = dataModels.get(dimension);
		    if (Float.isNaN(dataModel.getMinPreference()) && Float.isNaN(dataModel.getMaxPreference())) {
		      return null;
		    } else {
		      return new EstimatedPreferenceCapper(dataModel);
		    }
		  }
	
	  protected float doEstimatePreference(long theUserID, long[] theNeighborhood, long itemID, String dimension) throws TasteException {
		    if (theNeighborhood.length == 0) {		    	
		    	return Float.NaN;
		    }
		    DataModel dataModel = dataModels.get(dimension);
		    UserSimilarity similarity = similarities.get(dimension);
		    double preference = 0.0;
		    double totalSimilarity = 0.0;
		    int count = 0;
		    for (long userID : theNeighborhood) {
		    	
		      if (userID != theUserID) {
		        // See GenericItemBasedRecommender.doEstimatePreference() too
		        Float pref = dataModel.getPreferenceValue(userID, itemID);
		        //log.info("UserID: "+userID+" :: ItemID: "+itemID+" Pref: "+pref);
		        if (pref != null) {
		          double theSimilarity = similarity.userSimilarity(theUserID, userID);
		          //log.info("theSimilarity:"+theSimilarity);
		          if (!Double.isNaN(theSimilarity)) {		        	  
		        	preference += theSimilarity * pref;
		            totalSimilarity += theSimilarity;		            
		            count++;		            
		          }
		        }
		      }
		    }
		    
		    // Throw out the estimate if it was based on no data points, of course, but also if based on
		    // just one. This is a bit of a band-aid on the 'stock' item-based algorithm for the moment.
		    // The reason is that in this case the estimate is, simply, the user's rating for one item
		    // that happened to have a defined similarity. The similarity score doesn't matter, and that
		    // seems like a bad situation.
		    if (count <= 1) {		    	
		    	return Float.NaN;
		    }
		    float estimate = (float) (preference / totalSimilarity);
		    EstimatedPreferenceCapper capper = cappers.get(dimension);
		    if (capper != null) {
		      estimate = capper.capEstimate(estimate);
		    }		    
		      return estimate;
		  }
	
	
	  private final class Estimator implements TopItems.Estimator<Long> {
		    
		    private final long theUserID;
		    private HashMap<String, long[]> theNeighborhoods;
		    
		    Estimator(long theUserID, HashMap<String, long[]> theNeighborhoods) {
		      this.theUserID = theUserID;
		      this.theNeighborhoods=theNeighborhoods;
		    }
		    
		    @Override
		    public double estimate(Long itemID) throws TasteException {
		    	double totalEstimation=0.0;
		    	for(int i=0; i<dimensions.length; i++){
		    		double estimation = doEstimatePreference(theUserID, theNeighborhoods.get(dimensions[i]), itemID, dimensions[i]);
		    		totalEstimation=totalEstimation+ponderations.get(dimensions[i])*estimation;
		    	}
		        return totalEstimation;
		    }
	 }
	
	  private class MostSimilarEstimator implements TopItems.Estimator<Long> {
		    
		    private final long toUserID;
		    HashMap<String, UserSimilarity> similarities;
		    private final Rescorer<LongPair> rescorer;
		    
		    private MostSimilarEstimator(long toUserID, HashMap<String, UserSimilarity> similarities, Rescorer<LongPair> rescorer) {
		      this.toUserID = toUserID;
		      this.similarities=similarities;
		      this.rescorer = rescorer;
		    }
		    
		    @Override
		    public double estimate(Long userID) throws TasteException {
		      // Don't consider the user itself as a possible most similar user
		      if (userID == toUserID) {
		        return Double.NaN;
		      }
		      //double similarity;
		      if (rescorer == null) {
		        double totalSimilarity=0.0;
		    	  for(int i=0; i<dimensions.length; i++){
		        	double aux = similarities.get(dimensions[i]).userSimilarity(toUserID, userID);
		        	totalSimilarity=totalSimilarity+ponderations.get(dimensions[i])*aux;
		        }
		        return totalSimilarity;
		    	 
		      } else {
		        LongPair pair = new LongPair(toUserID, userID);
		        if (rescorer.isFiltered(pair)) {
		          return Double.NaN;
		        }
		        
		        double totalSimilarity=0.0;
		    	  for(int i=0; i<dimensions.length; i++){
		        	double aux = similarities.get(dimensions[i]).userSimilarity(toUserID, userID);
		        	totalSimilarity=totalSimilarity+ponderations.get(dimensions[i])*aux;
		        }
		        return rescorer.rescore(pair, totalSimilarity);
		      }
		    }
		  }
	
}
