package es.upm.multidimensional;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

import org.apache.mahout.cf.taste.common.TasteException;


public class Prueba {
	
	public static HashMap<String, Object> createData(String[] dimensions,
													 int numUsers, int numItems, 
													 int ratingsPerUser){
		HashMap<String, Object> hashMap = new HashMap<String, Object>();
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
		
		int numUsers=1000;
		int numItems=500;
		int ratingsPerUser=50;
		int myUsers[]={1}; //,90,190,400};
		
		String[] dimensions = {"dim_1","dim_2","dim_3"};
		
		HashMap<String, Object>  dataMap = createData(dimensions, numUsers,numItems,ratingsPerUser);
		Object dataArray[] = new Object[dataMap.size()];
		int k = 0;
		for(Object data : dataMap.values())
			dataArray[k++] = data;
		
		double[] ponderations = {0.3, 0.4, 0.3};
		HashMap<String, Double> ponderationMap = new HashMap<String, Double>();
		for (k = 0; k < ponderations.length; k++)
			ponderationMap.put(dimensions[k], ponderations[k]);
		
		String[] algorithms = {RecommendationGenerator.PEARSON,
				RecommendationGenerator.PEARSON, RecommendationGenerator.PEARSON};
		HashMap<String, String> algorithmMap = new HashMap<String, String>();
		for (k = 0; k < algorithms.length; k++)
			algorithmMap.put(dimensions[k], algorithms[k]);
		
		System.out.println("--------------------------- Test 1 ----------------------------------");
		RecommendationGenerator generator = new RecommendationGenerator(dataMap, ponderations, algorithms);
		print(generator, myUsers);
		System.out.println("--------------------------- Test 2 ----------------------------------");
		generator = new RecommendationGenerator(dataMap);
		print(generator, myUsers);
		System.out.println("--------------------------- Test 3 ----------------------------------");
		generator = new RecommendationGenerator(dimensions, dataArray);
		print(generator, myUsers);
		System.out.println("--------------------------- Test 4 ----------------------------------");
		String newAlgorithms[] = {RecommendationGenerator.TANIMOTO,
				RecommendationGenerator.EUCLIDIAN, RecommendationGenerator.COSINE};
		generator = new RecommendationGenerator(dimensions, dataArray, ponderations, newAlgorithms);
		print(generator, myUsers);
		System.out.println("--------------------------- Test 5 ----------------------------------");
		generator = new RecommendationGenerator(dataMap, ponderationMap, algorithmMap);
		print(generator, myUsers);
					
	}
	
	public static void print(RecommendationGenerator generator, int myUsers[]) throws TasteException {
		generator.showInfo();
		for(int myUser : myUsers) {
			HashMap<Long,Float> recommendations = generator.getRecommendations(myUser, 5);
			Iterator<Long> recIterator = recommendations.keySet().iterator();
			
			while(recIterator.hasNext()){
				Long key = (Long) recIterator.next();
				Float value = recommendations.get(key);
				System.out.println("Item number: "+key+", preference = "+value+".");				
			}
		}
	}
	
}
