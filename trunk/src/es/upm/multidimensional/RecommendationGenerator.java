package es.upm.multidimensional;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.CachingRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.SpearmanCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.TanimotoCoefficientSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.UncenteredCosineSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecommendationGenerator {
	
	// Constant values.
	public static final String EUCLIDIAN="EUCLIDIAN";
	public static final String LOGLIKELIHOOD="LOGLIKELIHOOD";
	public static final String PEARSON="PEARSON";
	public static final String SPEARMAN="SPEARMAN";
	public static final String TANIMOTO="TANIMOTO";
	public static final String COSINE="COSINE";
	private static final int defaultNeighbors=500;	
	private final Logger log = LoggerFactory.getLogger(RecommendationGenerator.class);	
	private static final long MIN_RELOAD_INTERVAL_MS = 60 * 1000L; // 1 minute?	
	
	// HashMaps, they are all indexed by an String object containing the name of the dimension.
	private HashMap<String, Object> dimensionData;
	//private HashMap<String, HashMap<Long, HashMap<Long, Float>>> hashMapData;
	private HashMap<String, Double> ponderations;
	private HashMap<String, String> similarityAlgorithms;
	private HashMap<String, Integer> similarityIntMap;
	
	// Control variables:
	public String[] dimensions; // Array of final names of every dimension.
	private long lastModified;
	private boolean refreshed;	
	private int neighbors; // Number of neighbors to consider in a NN-Algorithm.
	
	// Recommender instance:
	public Recommender recommender;
	
	
	/**
	 * Contructor for RecommendationGenerator
	 * @param mapData : array of dimension database for the recommender system.
	 * dimension ponderation: uniform ponderation of 1.0 for each dimension
	 * dimension similarity algorithm: PEARSON default algorithm for each dimension 
	 * @throws Exception
	 */
	public RecommendationGenerator(	String[] dimensions,
									Object[] mapData) throws Exception{
		// Verify the same dimensions length of items 
		if(dimensions.length != mapData.length)
			throw new Exception("We need to have the same number of dimensions for every" +
					" input argument of the constructor method.");
		
		// Identify the dimensions.
		this.dimensions = dimensions;
		
		HashMap<String, Object> hashMapData = new HashMap<String, Object>();
		HashMap<String, Double> ponderations=new HashMap<String, Double>();
		HashMap<String, String> similarityAlgorithms=new HashMap<String, String>();
		for(int i = 0; i < dimensions.length; i++)	{
			hashMapData.put(dimensions[i], mapData[i]);
			ponderations.put(dimensions[i], 1.0);
			similarityAlgorithms.put(dimensions[i], RecommendationGenerator.PEARSON);
		}		

		configure(hashMapData, ponderations, similarityAlgorithms);
		refresh();		
	}
	
	/**
	 * Contructor for RecommendationGenerator
	 * @param mapData : array of dimension database for the recommender system.
	 * dimension ponderation: uniform ponderation of 1.0 for each dimension
	 * dimension similarity algorithm: PEARSON default algorithm for each dimension 
	 * @throws Exception
	 */
	public RecommendationGenerator(	HashMap<String,Object> dimensionData) throws Exception{
		
		// Identify the dimensions.
		this.dimensions = new String[dimensionData.size()];
		int k = 0;
		HashMap<String, Double> ponderations=new HashMap<String, Double>();
		HashMap<String, String> similarityAlgorithms=new HashMap<String, String>();
		for(String dimension : dimensionData.keySet())	{
			dimensions[k++] = dimension;
			ponderations.put(dimension, 1.0);
			similarityAlgorithms.put(dimension, RecommendationGenerator.PEARSON);
		}		

		configure(dimensionData, ponderations, similarityAlgorithms);
		refresh();		
	}
	
	/**
	 * Contructor for RecommendationGenerator
	 * @param mapData : array of dimension database for the recommender system.
	 * @param dimensionPonderations : array of Double objects that weights each dimension.
	 * @param dimensionSimAlgorithms : array of String objects that specify which similarity algorithm we want to use in ecah dimension.
	 * @throws Exception
	 */
	public RecommendationGenerator(	String[] dimensions, Object[] mapData,
									double[] dimensionPonderations,
									String[] dimensionSimAlgorithms) throws Exception{
		// Verify the same dimensions length of items 
		if(dimensions.length != mapData.length || dimensions.length != dimensionPonderations.length
				|| dimensions.length != dimensionSimAlgorithms.length)
			throw new Exception("We need to have the same number of dimensions for every" +
					" input argument of the constructor method.");
		
		// Identify the dimensions.
		this.dimensions = dimensions;
		
		HashMap<String, Object> hashMapData = new HashMap<String, Object>();
		HashMap<String, Double> ponderations=new HashMap<String, Double>();
		HashMap<String, String> similarityAlgorithms=new HashMap<String, String>();
		for(int i = 0; i < dimensions.length; i++)	{
			hashMapData.put(dimensions[i], mapData[i]);
			ponderations.put(dimensions[i], dimensionPonderations[i]);
			similarityAlgorithms.put(dimensions[i],dimensionSimAlgorithms[i]);
		}		

		configure(hashMapData, ponderations, similarityAlgorithms);
		refresh();		
	}
	
	
	/**
	 * Contructor for RecommendationGenerator
	 * @param hashMapData : map of dimension database for the recommender system.
	 * @param dimensionPonderations : array of Double objects that weights each dimension.
	 * @param dimensionSimAlgorithms : array of String objects that specify which similarity algorithm we want to use in ecah dimension.
	 * @throws Exception
	 */
	public RecommendationGenerator(	HashMap<String, Object> hashMapData,
									double[] dimensionPonderations,
									String[] dimensionSimAlgorithms) throws Exception{
		if(dimensionPonderations.length != hashMapData.size() || 
				dimensionPonderations.length != dimensionSimAlgorithms.length)
			throw new Exception("We need to have the same number of dimensions for every" +
					" input argument of the constructor method.");
		
		// Identify the dimensions.			
		dimensions=new String[hashMapData.size()];
		Iterator<String> dimIterator = hashMapData.keySet().iterator();
		int k=0;
		while(dimIterator.hasNext()){			
			dimensions[k++] = dimIterator.next();
		}		
		
//		this.dimensions=(String[]) hashMapData.keySet().toArray();		
		HashMap<String, Double> ponderations=new HashMap<String, Double>();
		HashMap<String, String> similarityAlgorithms=new HashMap<String, String>();		
		for(int i=0; i<dimensions.length; i++){
			ponderations.put(dimensions[i], dimensionPonderations[i]);
			similarityAlgorithms.put(dimensions[i],dimensionSimAlgorithms[i]);
		}
		configure(hashMapData, ponderations, similarityAlgorithms);
		refresh();		
	}
	
	/**
	 * Constructor for RecommendationGenerator
	 * @param hashMapData : database for the recommender system.
	 * @param ponderations : HashMap of Double objects indexed by String objects (every String object stores the name of a dimension).  
	 * @param similarityAlgorithms : HashMap of String objects indexed by String objects (every String object stores the name of a dimension).
	 * @throws Exception
	 */
	public RecommendationGenerator(	HashMap<String, Object> hashMapData,
									HashMap<String, Double> ponderations,
									HashMap<String, String> similarityAlgorithms) throws Exception{
		configure(hashMapData, ponderations, similarityAlgorithms);
		refresh();		
	}
	
	private void configure 	(HashMap<String, Object> mapData,
			HashMap<String, Double> ponderations,HashMap<String, String> similarityAlgorithms) 
			throws Exception {		
		log.info("Starting the recommendation engine.");		
		// Check that the number of dimensions is correct.
		if( (mapData.size()!=ponderations.size() || similarityAlgorithms.size()!=ponderations.size() ))
				throw new Exception("We need to have the same number of dimensions for every " +
						"input argument of the constructor method.");
		//Check that mapData values corresponds to DataModel or HashMap<Long, HashMap<Long, Float>> instances
		for(Object data : mapData.values()) {
			if(!(data instanceof DataModel)) {
				if(!(data instanceof HashMap)) {
					throw new Exception("The dimensional data have to be instances of DataModel " +
							"or HashMap<Long, HashMap<Long, Float>>");
				}
			}
		}
		
		// Identify the dimensions.
		if(dimensions == null) {
			dimensions = new String[mapData.size()];
			Iterator<String> dimIterator = mapData.keySet().iterator();
			int k=0;
			while(dimIterator.hasNext()){			
				String dim = dimIterator.next();
				dimensions[k++] = dim;				
				if(!ponderations.containsKey(dim))
					 throw new Exception("Different dimension names between hasMapData and ponderations.");
				if(!similarityAlgorithms.containsKey(dim))
					 throw new Exception("Different dimension names between hasMapData and similarity algorithm.");
			}					
		}
		
		// Validates the names of the chosen algorithms
		for(int i=0; i<similarityAlgorithms.size(); i++){
			String a = similarityAlgorithms.get(dimensions[i]);
			if((a!=EUCLIDIAN)&&(a!=LOGLIKELIHOOD)&&(a!=PEARSON)&&(a!=SPEARMAN)&&(a!=TANIMOTO)&&(a!=COSINE)){
				throw new Exception("\""+a+"\""+" is not a valid name for a similarity " +
						"algorithm for the dimension: \""+dimensions[i]+"\".");
			}
		}
		log.info("The dimensions of the input data seem to be correct");
		// Initialization of variables.
		this.dimensionData=mapData;
		this.ponderations=ponderations;
		this.similarityAlgorithms=similarityAlgorithms;
		this.similarityIntMap= new HashMap<String, Integer>();
		similarityIntMap.put(EUCLIDIAN,1);
		similarityIntMap.put(LOGLIKELIHOOD,2);
		similarityIntMap.put(PEARSON,3);
		similarityIntMap.put(SPEARMAN,4);
		similarityIntMap.put(TANIMOTO,5);
		similarityIntMap.put(COSINE,6);
		this.neighbors=defaultNeighbors;
		this.refreshed=false;
	}
	
	/**
	 * Refresh all the inner information of the recommender system.
	 * @throws TasteException
	 * @throws Exception
	 */
	public void refresh() throws TasteException, Exception{
		if((!refreshed)&&(Calendar.getInstance().getTimeInMillis()>=(this.lastModified+MIN_RELOAD_INTERVAL_MS))){
		
			log.info("Refreshing the information of the recommendation engine.");
			HashMap<String, DataModel> dataModels= new HashMap<String, DataModel>();
			HashMap<String, UserSimilarity> similarities= new HashMap<String, UserSimilarity>();
			HashMap<String, UserNeighborhood> neighborhoods = new HashMap<String,UserNeighborhood>();
			
			for(int i=0; i<dimensions.length;i++ ){
				String dimension = dimensions[i];
				log.info("Regarding the "+"\""+" dimension:");
				DataModel dataModel;
				if(dimensionData.get(dimension) instanceof DataModel) {
					dataModel = (DataModel) dimensionData.get(dimension);
					dataModels.put(dimension, dataModel);
				} else {
					HashMap<Long, HashMap<Long, Float>> auxHashMap = 
						(HashMap<Long, HashMap<Long, Float>>) dimensionData.get(dimension);
					checkHashMap(auxHashMap);					
					log.info("Creating DataModel instance for the "+"\""+dimension+"\""+" dimension.");
					dataModel = createDataModel(auxHashMap);
					dataModels.put(dimension, dataModel);
				}
				log.info("Creating UserSimilarity instance for the "+"\""+dimension+"\""+" dimension.");
				UserSimilarity userSimilarity = createUserSimilarity(similarityAlgorithms.get(dimension),dataModel);
				similarities.put(dimension,userSimilarity);
				log.info("Creating UserNeighborhood instance for the "+"\""+dimension+"\""+" dimension.");
				UserNeighborhood userNeighborhood = new NearestNUserNeighborhood(neighbors,userSimilarity,dataModel);
				neighborhoods.put(dimension,userNeighborhood);			
				log.info("UserNeighborhood instance created successfully.");
			}		
			log.info("All inner instances created successfully.");
			log.info("Creating the Recommender instance:");
			recommender = new CachingRecommender(new MultidimensionalRecommender(dataModels, similarities,
																							neighborhoods, ponderations, 
																							dimensions,log));
			refreshed=true;
			lastModified=Calendar.getInstance().getTimeInMillis();
			log.info("Recommendation engine refreshed: ready to infer recommendations.");
		}
	}
	
	/**
	 * Obtain recommendations for user 'userID'.
	 * @param userID : user ID for whom we want to infer a list of recommendations.
	 * @param numOfRecommendations : number of recommendations we want to derive.
	 * @return list of pairs (item ID, value of preference).
	 * @throws TasteException 
	 */
	public HashMap<Long,Float> getRecommendations(long userID, int numOfRecommendations) throws TasteException{
		log.info("Getting recommendations. This step may take a while...");
		System.out.println(recommender);
		List<RecommendedItem> recommendations = recommender.recommend(userID, numOfRecommendations);
		HashMap<Long,Float> hashMap = new HashMap<Long, Float>();
		if(recommendations.isEmpty()){
			log.info("There are no recommendations to give.");
			return hashMap;
		}
		for (RecommendedItem rec: recommendations){
	    	hashMap.put(rec.getItemID(), rec.getValue());
	    }
		log.info("Recommendations derived successfully.");
		return hashMap;
	}
	
	/**
	 * Remove the rating given by 'userID' to 'itemID' regarding to the 'dim' dimension.
	 * @param dim : dimension for which we want to remove a rating.
	 * @param userID : user for which we want to remove a rating.
	 * @param itemID : item for which we want to remove a rating.
	 */
	public void removeRating(String dim, long userID, long itemID){
		if(dimensionData.containsKey(dim)){//
			throw new IllegalArgumentException("This dimension does not exist.");
		}
		Object map = dimensionData.get(dim);
		if(! (map instanceof HashMap)) {
			throw new IllegalArgumentException("This dimension uses Data Model directly and not hash Map");
		}
		HashMap<Long,HashMap<Long,Float>> hashMapByDimension = (HashMap<Long,HashMap<Long,Float>>) map;		
		HashMap<Long,Float> hashMapByUid = (HashMap<Long,Float>) hashMapByDimension.get(userID);
		if(hashMapByUid==null){//
			throw new IllegalArgumentException("This user has not rated this dimension.");
		}
		hashMapByUid.remove(itemID);
		//hashMapByDimension.put(userID, hashMapByUid);
		//hashMapData.put(dim, hashMapByDimension);
		this.refreshed=false;
		log.info("You have removed the rating of item number "+itemID+" for user number "+userID+" at the dimension: \""+dim+"\".");
		log.info("You may refresh now.");
	}
	
	/**
	 * Put a rating into the database for user 'userID' and item 'itemID' regarding the 'dim' dimension.
	 * @param dim : dimension for which we want to put a rating.
	 * @param userID : user for which we want to put a rating.
	 * @param itemID : item for which we want to put a rating.
	 * @param value : value of the rating.
	 */
	public void putRating(String dim, long userID, long itemID, float value){
		if(dimensionData.containsKey(dim)){//
			throw new IllegalArgumentException("This dimension does not exist.");
		}
		Object map = dimensionData.get(dim);
		if(! (map instanceof HashMap)) {
			throw new IllegalArgumentException("This dimension uses Data Model directly and not hash Map");
		}
		HashMap<Long,HashMap<Long,Float>> hashMapByDimension = (HashMap<Long,HashMap<Long,Float>>) map;
		HashMap<Long,Float> hashMapByUid = (HashMap<Long,Float>) hashMapByDimension.get(userID);
		if(hashMapByUid==null){//new user
			HashMap<Long,Float> newUserHashMap = new HashMap<Long,Float>();
			newUserHashMap.put(itemID, value);
			hashMapByDimension.put(userID, newUserHashMap);
			//hashMapData.put(dim, hashMapByDimension);
			return;
		}
		hashMapByUid.put(itemID,value);
		//hashMapByDimension.put(userID,hashMapByUid);
		//hashMapData.put(dim, hashMapByDimension);
		this.refreshed=false;
		log.info("You have put the rating of "+value+" to item number "+itemID+" for user number "+userID+" at the dimension: \""+dim+"\".");
		log.info("You may refresh now.");
	}
	
	/**
	 * Set the similarity algorithm we want to use for a given dimension.
	 * @param dimension : dimension for which we want to choose a similarity algorithm.
	 * @param similarityAlgorithm : similarity algorithm we want to assign to the given dimension.
	 */
	public void setSimilarity(String dimension, String similarityAlgorithm){	
		if (similarityAlgorithms.containsKey(dimension)){
			similarityAlgorithms.put(dimension, similarityAlgorithm);
			this.refreshed=false;
			log.info("You have set \""+similarityAlgorithm+"\" algorithm for \""+dimension+"\" dimension.");
			log.info("You may refresh now.");
		} else
			throw new IllegalArgumentException("This dimension does not exist.");
	}
	
	/**
	 * Sets the number of neighbors to consider in the k-Nearest Neighbors Algorithm of the recommender system.
	 * @param neighbors : number of neighbors to consider.
	 */
	public void setNeighbors(int neighbors){
		this.neighbors=neighbors;
		log.info("You have set a "+neighbors+"-Nearest Neighbors algorithm for the recommender system.");
		log.info("You may refresh now.");		
	}
	
	/**
	 * Shows the information about the recommender system options chosen. 
	 */
	public void showInfo(){
		System.out.println("*********************************************************************************************");
		System.out.println("THESE ARE THE VALUES OF THE RECOMMENDER SYSTEM:");
		for(int i=0; i<dimensions.length;i++ ){
			System.out.println("You have set \""+similarityAlgorithms.get(dimensions[i]) +
					"\" similarity calculation algorithm for \""+dimensions[i]+"\" dimension.");
		}		
		System.out.println("The recommender is based on a "+neighbors+"-Nearest Neighbors Algorithm.");
		if(!refreshed)
			System.out.println("The data have been modified since the creation of the Recommender instance. You should invoke refresh().");		
		System.out.println("*********************************************************************************************");				
	}
	/**
	 * Check if the input HashMap is given in a correct form.
	 * @param hashMap: input HashMap.
	 */
	private void checkHashMap(HashMap hashMap){
		log.info("Checking if the input hash map data table has the correct dimensions.");
		if(hashMap.size()==0){
			log.info("Empty hash map");
			return;
		}
		Iterator iterator_1 = hashMap.keySet().iterator();
		while(iterator_1.hasNext()){
			Object key_1 = iterator_1.next();
			if(!(key_1 instanceof Long)){
				throw new IllegalArgumentException("All user ID's should be instances of Long.");
			}
			Object object_1=hashMap.get(key_1);
			if(!(object_1 instanceof HashMap)){
				throw new IllegalArgumentException("Input HashMap should contain HashMap instances.");
			}
			HashMap hashMap_1=(HashMap) object_1;
			Iterator iterator_2= hashMap_1.keySet().iterator();
			while(iterator_2.hasNext()){
				Object key_2 = iterator_2.next();
				if(!(key_2 instanceof Long)){
					throw new IllegalArgumentException("All item ID's should be instances of Long.");
				}
				Object object_2=hashMap_1.get(key_2);
				if(!(object_2 instanceof Float)){
					throw new IllegalArgumentException("All preference values should be instances of Float.");
				}
			}		
		}
		log.info("Correct HashMap.");
	}
	
	/**
	 * Creates a DataModel instance given a input HashMap.
	 * @param hashMap : input HashMap.
	 * @return DataModel instance
	 */
	private DataModel createDataModel(HashMap<Long, HashMap<Long, Float>> hashMap){
		FastByIDMap<PreferenceArray> fastMap = new FastByIDMap<PreferenceArray>();
		Iterator<Long> hashMapIterator=hashMap.keySet().iterator();
		while(hashMapIterator.hasNext()){
			Long idUser = hashMapIterator.next();
			Map<Long, Float> hashMapByUid = (HashMap<Long, Float>) hashMap.get(idUser);
			Iterator<Long> hashMapByUidIterator = hashMapByUid.keySet().iterator();
			List<Preference> list = new ArrayList<Preference>();
			while(hashMapByUidIterator.hasNext()){
				Long idItem = hashMapByUidIterator.next();
				Float ratingForIdItem = hashMapByUid.get(idItem);
				Preference p = new GenericPreference(idUser, idItem, ratingForIdItem);
				list.add(p);
			}
			PreferenceArray prefArray=new GenericUserPreferenceArray(list);
			fastMap.put(idUser, prefArray);
		}
		DataModel dataModel = new GenericDataModel(fastMap);
		log.info("DataModel instance created successfully.");
		return dataModel;
	}
	
	/**
	 * 
	 * @param similarity
	 * @param dataModel
	 * @return
	 * @throws TasteException
	 */
	private UserSimilarity createUserSimilarity(String similarity,DataModel dataModel) throws TasteException {
		int sim=similarityIntMap.get(similarity);
		switch(sim){
		case 1:  
			log.info("Euclidean distance UserSimilarity instance created successfully."); 
			return new EuclideanDistanceSimilarity(dataModel);
		case 2:  
			log.info("Log Likelihood UserSimilarity instance created successfully.");
			return new LogLikelihoodSimilarity(dataModel); 
		case 3:  
			log.info("Pearson correlation UserSimilarity instance created successfully.");
			return new PearsonCorrelationSimilarity(dataModel);
		case 4:  
			log.info("Spearman correlation UserSimilarity instance created successfully.");
			return new SpearmanCorrelationSimilarity(dataModel);
		case 5:  
			log.info("Tanimoto coefficient UserSimilarity instance created successfully.");
			return new TanimotoCoefficientSimilarity(dataModel); 
		case 6:  
			log.info("Uncentered cosine UserSimilarity instance created successfully.");
			return new UncenteredCosineSimilarity(dataModel);
		default: 
			log.warn("Default value: Pearson correlation UserSimilarity instance returned.");
			return new PearsonCorrelationSimilarity(dataModel);
		}
	}
	
	public Logger getLog() {
		return log;
	}

}
