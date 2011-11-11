import java.util.Date;
import java.util.HashMap;

import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.jdbc.MySQLJDBCDataModel;

import es.upm.multidimensional.RecommendationGenerator;


public class ExampleExecutor {
	public static void main( String args[] ) throws Exception {
		String database_server = "<database_server>";
		String db_name = "<db_name>";
		String db_user = "<db_user>";
		String db_password = "<db_password>";
		
		System.out.println("----------------------------- Test 0 --------------------------------------------");
		System.out.println("--------------- Creating the recommendationGenerator Object ---------------------");		
		String[] dimensions = {"READ","LIKED"};
		HashMap<String, Object> recommendationData = new HashMap<String, Object> ();
		recommendationData.put(dimensions[0], new HashMap<Long,HashMap<Long,Float>>());
		recommendationData.put(dimensions[1], new HashMap<Long,HashMap<Long,Float>>());		
		MySQLInterface.connectToDatabase(database_server, db_name, db_user, db_password);
		Date timestamp = new Date();
		MySQLInterface.takeRecommendationData (recommendationData, dimensions, 
				"users_messages", "iduser", "idmessage", "liked");
		HashMap<String, Double > ponderations = new HashMap<String, Double> ();
		ponderations.put(dimensions[0],1.0);
		ponderations.put(dimensions[1],1.0);
		HashMap<String, String > similarityAlgorithms = new HashMap<String, String> ();
		similarityAlgorithms.put(dimensions[0],RecommendationGenerator.TANIMOTO);
		similarityAlgorithms.put(dimensions[1],RecommendationGenerator.EUCLIDIAN);	
		RecommendationGenerator recommender = null;
		try {
			recommender = new RecommendationGenerator(recommendationData,ponderations,similarityAlgorithms);
			recommender.showInfo();			
			for(Long userID : ((HashMap<Long,HashMap<Long,Float>>) recommendationData.get(dimensions[0])).keySet()) {
				HashMap<Long, Float> recommendationUserData = recommender.getRecommendations(userID,30);
				System.out.println(userID+" recommendation list: "+recommendationUserData);				
				MySQLInterface.updateRecommendationUserData(true, userID, recommendationUserData, 
						"users_recommendations","iduser","idmessage","user_afinity");						
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("------------- Updating Data in the recommendationGenerator Object ---------------");
		Date newTimestamp = new Date();		
		MySQLInterface.updateRecommendationData (recommender, dimensions, timestamp, 
				"users_messages","iduser", "idmessage", "liked","updatedate");				
		try {
			recommender.showInfo();
			recommender.refresh();
			for(Long userID : ((HashMap<Long,HashMap<Long,Float>>) recommendationData.get(dimensions[0])).keySet()) {
				HashMap<Long, Float> recommendationUserData = recommender.getRecommendations(userID,30);
				System.out.println(userID+" recommendation list: "+recommendationUserData);
				MySQLInterface.updateRecommendationUserData(true, userID, recommendationUserData, 
						"users_recommendations","iduser","idmessage","user_afinity");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		timestamp = newTimestamp;
		
		System.out.println("---------- Updating Data by HashMap in the recommendationGenerator Object ------------");
		HashMap<String, HashMap<Long,HashMap<Long,Float>>> newRecommendationData = 
				new HashMap<String, HashMap<Long,HashMap<Long,Float>>> ();
		newRecommendationData.put(dimensions[0], new HashMap<Long,HashMap<Long,Float>>());
		newRecommendationData.put(dimensions[1], new HashMap<Long,HashMap<Long,Float>>());		
		newTimestamp = new Date();		
		MySQLInterface.updateRecommendationData (newRecommendationData, dimensions,
				timestamp, "users_messages", "iduser", "idmessage", "liked","updatedate");
		recommender.putRating((HashMap<String, HashMap<Long, HashMap<Long, Float>>>)newRecommendationData);
		//HashMap<String, HashMap<Long,HashMap<Long,Float>>> auxRecommendationData = 
		//	new HashMap<String, HashMap<Long,HashMap<Long,Float>>> ();
		//for(String key : newRecommendationData.keySet()) {
		//	auxRecommendationData.put(key, (HashMap<Long,HashMap<Long,Float>>) newRecommendationData.get(key));
		//}
		try {
			recommender.showInfo();
			recommender.refresh();
			for(Long userID : ((HashMap<Long,HashMap<Long,Float>>) recommendationData.get(dimensions[0])).keySet()) {
				HashMap<Long, Float> recommendationUserData = recommender.getRecommendations(userID,30);
				System.out.println(userID+" recommendation list: "+recommendationUserData);
				MySQLInterface.updateRecommendationUserData(true, userID, recommendationUserData,
						"users_recommendations","iduser","idmessage","user_afinity");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}		
		MySQLInterface.closeConnectionDatabase();
		
		
		
		/*System.out.println("----------------------------- Test 1: HashMap ----------------------------");		
		HashMap<String, Object> dimesionalUsers = new HashMap<String, Object> ();
		double ponderationss[] = {1};
		String similarities[] = {RecommendationGenerator.EUCLIDIAN};	
		MySQLInterface.connectToDatabase();
		HashMap<Long,HashMap<Long,Float>> users = new HashMap<Long,HashMap<Long,Float>>(); 
		MySQLInterface.takeRecommendationData(users, "users_messages", "iduser", "idmessage", "liked");
		MySQLInterface.closeConnectionDatabase();
		//System.out.println(users);
		System.out.println("users size: "+users.size());
		for(Map<Long,Float> messages : users.values()) {
			System.out.println("messages size: "+messages.size());
		}
		dimesionalUsers.put("liked", users);			
		try {
			RecommendationGenerator recommenderX = 
				new RecommendationGenerator(dimesionalUsers,ponderationss,similarities);
			recommenderX.showInfo();
			for(Long userID : users.keySet()) {
				System.out.println(userID+" recommendation list: "+recommender.getRecommendations(userID,30));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}*/
		
		
		System.out.println("----------------------------- Test 2: DataModel ----------------------------");
		MySQLJDBCDataModel dataModel = 
			new MySQLJDBCDataModel(MySQLInterface.getDataSource(database_server, db_name, 
			db_user, db_password),"users_messages","iduser","idmessage","liked");
		System.out.println("dataModel:"+dataModel.getNumItems()+" "+dataModel.getNumUsers()+
				" "+dataModel.getMaxPreference());
		HashMap<String, Object> dimesionalUserss = new HashMap<String, Object> ();
		double ponderationsss[] = {1};
		String similaritiess[] = {RecommendationGenerator.EUCLIDIAN};
		dimesionalUserss.put("liked", dataModel);		
		try {
			RecommendationGenerator recommenderX = 
				new RecommendationGenerator(dimesionalUserss,ponderationsss,similaritiess);
			recommenderX.showInfo();
			LongPrimitiveIterator  userIDs = dataModel.getUserIDs();
			while(userIDs.hasNext()) {
				Long userID = userIDs.next();
				System.out.println(userID+" recommendation list: "+recommenderX.getRecommendations(userID,30));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
