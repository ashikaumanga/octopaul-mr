package es.upm.multidimensional;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;


public class Prueba {
	
	public static HashMap<String, HashMap<Long, HashMap<Long, Float>>> createData(String[] dimensions,
																				  int numUsers, int numItems, 
																				  int ratingsPerUser){
		HashMap<String, HashMap<Long, HashMap<Long, Float>>> hashMap = new HashMap<String, HashMap<Long, HashMap<Long, Float>>>();
		for(int k=0; k<dimensions.length; k++){
			HashMap<Long, HashMap<Long,Float>> hashMapByDim = new HashMap<Long, HashMap<Long,Float>>();
			Random randomGenerator = new Random();
			for(long i=1; i<=numUsers; i++){
				HashMap<Long,Float> auxHashMap = new HashMap<Long,Float>();
				for(int j=0; j<ratingsPerUser; j++){				
					float randomValue=randomGenerator.nextFloat();
					long randomItem= (long) (1.0+Math.floor(Math.random()*numItems));
					auxHashMap.put(randomItem, randomValue);				
				}
				hashMapByDim.put(i, auxHashMap);
			}		
			hashMap.put(dimensions[k],hashMapByDim);
		}				
		return hashMap;
	}
	
	public static void main(String[] args) throws Exception{
		
		int numUsers=10000;
		int numItems=500;
		int ratingsPerUser=50;
		int myUser=90;
		
		String[] dimensions = {"dim_1","dim_2","dim_3"};
		double[] ponderations = {0.3, 0.4, 0.3};
		String[] algorithms = {RecommendationGenerator.PEARSON,RecommendationGenerator.PEARSON, RecommendationGenerator.PEARSON};
		
		HashMap<String,HashMap<Long, HashMap<Long, Float>>>  hashMap = createData(dimensions, numUsers,numItems,ratingsPerUser);
		
		RecommendationGenerator generator = new RecommendationGenerator(hashMap, ponderations, algorithms);
		
		//generator.showInfo();
		HashMap<Long,Float> recommendations = generator.getRecommendations(myUser, 5);
		Iterator<Long> recIterator = recommendations.keySet().iterator();
		
		while(recIterator.hasNext()){
			Long key = (Long) recIterator.next();
			Float value = recommendations.get(key);
			System.out.println("Item number: "+key+", preference = "+value+".");				
		}		
			
	}
}
