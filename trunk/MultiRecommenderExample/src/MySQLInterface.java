import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.spi.NamingManager;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.jdbc.MySQLJDBCDataModel;
//import org.apache.mahout.cf.taste.impl.model.jdbc.MySQLJDBCDataModel;

import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import es.upm.multidimensional.RecommendationGenerator;

public class MySQLInterface {
	private static Connection con;
	private static Statement stmt;
	private static PreparedStatement deleteRecommendationData;
	private static PreparedStatement insertRecommendationData;
	private static String database_server;
	private static String db_name;
	private static String db_user;
	private static String db_password;

	/**
	 * Connects to the database
	 */
	public static void connectToDatabase(String database_server, String db_name,
			String db_user, String db_password) {
		try {
			MySQLInterface.database_server = database_server;
			MySQLInterface.db_name = db_name;
			MySQLInterface.db_user = db_user;
			MySQLInterface.db_password = db_password;
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection(
					"jdbc:mysql://"+database_server+"/"+db_name, db_user, db_password);			
			stmt = con.createStatement();			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void setUsers (HashMap<Long,HashMap<Long,Float>> users, String usersTable, String userIDColumn) {
		if(users == null)
			users = new HashMap<Long,HashMap<Long,Float>>();
		String query = "SELECT "+userIDColumn+" from "+usersTable;
		try {
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
			    int user = rs.getInt(userIDColumn);
			    HashMap<Long,Float> hashMapByUid = (HashMap<Long,Float>)users.get(Long.valueOf(user));
			    if (hashMapByUid == null) {			      
			      users.put(Long.valueOf(user), new HashMap<Long,Float>());			      
			    } 
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static HashMap<Long,HashMap<Long,Float>> takeRecommendationData (HashMap<Long,HashMap<Long,Float>> users,
			String table, String userColumn, String messageColumn, String likedColumn) {
		try {
			if((con == null || con.isClosed()) && database_server != null) {
				connectToDatabase(database_server, db_name, db_user, db_password);
			}
		} catch (SQLException e1) {			
			e1.printStackTrace();
		}
		if(users == null)
			users = new HashMap<Long,HashMap<Long,Float>>();
		String query = "SELECT "+userColumn+","+messageColumn+","+likedColumn+
			" from "+table+" order by "+userColumn+","+messageColumn;
		try {
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
			    int user = rs.getInt(userColumn);
			    int message = rs.getInt(messageColumn);
			    int liked = rs.getInt(likedColumn);
			    
			    HashMap<Long,Float> hashMapByUid = (HashMap<Long,Float>)users.get(Long.valueOf(user));
			    if (hashMapByUid == null) {
			    	HashMap<Long,Float> newUserHashMap = new HashMap<Long,Float>();
			      newUserHashMap.put(Long.valueOf(message), Float.valueOf(liked));
			      users.put(Long.valueOf(user), newUserHashMap);			      
			    } else
			    	hashMapByUid.put(Long.valueOf(message), Float.valueOf(liked));
			}			
			return users;

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static void takeRecommendationData (HashMap<String,Object> recommendationData,
			String dimensions[],String table, String userColumn,String messageColumn, String likedColumn) {
		try {
			if((con == null || con.isClosed()) && database_server != null) {
				connectToDatabase(database_server, db_name, db_user, db_password);
			}
		} catch (SQLException e1) {			
			e1.printStackTrace();
		}
		String query = "SELECT "+userColumn+","+messageColumn+","+likedColumn+
			" from "+table+" order by "+userColumn+","+messageColumn;
		try {
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
			    int user = rs.getInt(userColumn);
			    int message = rs.getInt(messageColumn);
			    int liked = rs.getInt(likedColumn);
			    HashMap<Long,HashMap<Long,Float>> readData = 
			    	(HashMap<Long,HashMap<Long,Float>>) recommendationData.get(dimensions[0]);
			    HashMap<Long,Float> hashMapByUid = (HashMap<Long,Float>)readData.get(Long.valueOf(user));
			    if (hashMapByUid == null) {
			    	HashMap<Long,Float> newUserHashMap = new HashMap<Long,Float>();
			    	newUserHashMap.put(Long.valueOf(message), 1f);
			    	readData.put(Long.valueOf(user), newUserHashMap);			      
			    } else
			    	hashMapByUid.put(Long.valueOf(message), Float.valueOf(liked));
			    if(liked != 0) {
				    HashMap<Long,HashMap<Long,Float>> likedData = 
				    	(HashMap<Long,HashMap<Long,Float>>) recommendationData.get(dimensions[1]);
				    hashMapByUid = (HashMap<Long,Float>)likedData.get(Long.valueOf(user));
				    if (hashMapByUid == null) {
				    	HashMap<Long,Float> newUserHashMap = new HashMap<Long,Float>();
				    	newUserHashMap.put(Long.valueOf(message), Float.valueOf(liked));
				    	likedData.put(Long.valueOf(user), newUserHashMap);			      
				    } else
				    	hashMapByUid.put(Long.valueOf(message), Float.valueOf(liked));
			    }			    
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}		
	}
	
	public static void closeConnectionDatabase() {
		try {
			if(stmt != null)
				stmt.close();
			if(con != null)
				con.close();
		} catch (SQLException e) {
			
		}
	}
	public static DataSource getDataSource(String database_server, 
			String db_name,String db_user, String db_password) {
		/*InitialContext ctx;
		try {
			ctx = new InitialContext();			
			return (DataSource) ctx.lookup("java:comp/env/jdbc/investalia");			 
		} catch (NamingException e1) {
			e1.printStackTrace();
		}*/	
		MysqlDataSource dataSource = new MysqlDataSource();
		dataSource.setAllowMultiQueries(false);
		//dataSource.setLocalSocketAddress(address)
		//dataSource.setSocketFactory(name)
		dataSource.setAutoReconnect(true);
		//dataSource.setPortNumber(p)
		//dataSource.setSocketFactory(name)
		//dataSource.set
    	dataSource.setUser("readUser");
    	dataSource.setPassword("bobafett1");
    	dataSource.setDatabaseName("investalia");
    	dataSource.setServerName("sondheim");
    	return dataSource;
	}
	
	public static void updateRecommendationData (HashMap<String,HashMap<Long,HashMap<Long,Float>>> recommendationData,String dimensions[], 
			Date date, String table, String userColumn,String messageColumn, String likedColumn, String updateDate) {
		try {
			if((con == null || con.isClosed()) && database_server != null) {
				connectToDatabase(database_server, db_name, db_user, db_password);
			}
		} catch (SQLException e1) {			
			e1.printStackTrace();
		}
		String query = "SELECT "+userColumn+","+messageColumn+","+likedColumn+
		" from "+table+" where \'"+updateDate+" > \'"+date+"\' order by "+userColumn+","+messageColumn;
		try {
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
			    int user = rs.getInt(userColumn);
			    int message = rs.getInt(messageColumn);
			    int liked = rs.getInt(likedColumn);
			    HashMap<Long,HashMap<Long,Float>> readData = 
			    	(HashMap<Long,HashMap<Long,Float>>) recommendationData.get(dimensions[0]);
			    HashMap<Long,Float> hashMapByUid = (HashMap<Long,Float>)readData.get(Long.valueOf(user));
			    if (hashMapByUid == null) {
			    	HashMap<Long,Float> newUserHashMap = new HashMap<Long,Float>();
			    	newUserHashMap.put(Long.valueOf(message), 1f);
			    	readData.put(Long.valueOf(user), newUserHashMap);			      
			    } else
			    	hashMapByUid.put(Long.valueOf(message), Float.valueOf(liked));
			    if(liked != 0) {
				    HashMap<Long,HashMap<Long,Float>> likedData = 
				    	(HashMap<Long,HashMap<Long,Float>>) recommendationData.get(dimensions[1]);
				    hashMapByUid = (HashMap<Long,Float>)likedData.get(Long.valueOf(user));
				    if (hashMapByUid == null) {
				    	HashMap<Long,Float> newUserHashMap = new HashMap<Long,Float>();
				    	newUserHashMap.put(Long.valueOf(message), Float.valueOf(liked));
				    	likedData.put(Long.valueOf(user), newUserHashMap);			      
				    } else
				    	hashMapByUid.put(Long.valueOf(message), Float.valueOf(liked));
			    }			    
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void updateRecommendationData (RecommendationGenerator recommender,String dimensions[], 
			Date date, String table, String userColumn,String messageColumn, String likedColumn, String updateDate) {
		try {
			if((con == null || con.isClosed()) && database_server != null) {
				connectToDatabase(database_server, db_name, db_user, db_password);
			}
		} catch (SQLException e1) {			
			e1.printStackTrace();
		}
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String query = "SELECT "+userColumn+","+messageColumn+","+likedColumn+
		" from "+table+" where "+updateDate+" > \'"+dateFormat.format(date)+"\' order by "+userColumn+","+messageColumn;
		try {
			//System.out.println(query);
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
			    long user = rs.getLong(userColumn);
			    long message = rs.getLong(messageColumn);
			    float liked = rs.getFloat(likedColumn);
			    recommender.putRating(dimensions[0], user, message, 1);
			    if(liked != 0)
			    	recommender.putRating(dimensions[1], user, message, liked);		    
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void updateRecommendationUserData (boolean firstDelete, long userID, HashMap<Long, Float> recommendationUserData,
			String userRecommendationTable, String userIdColumn, String messageIdColumn, String userAfinityColumn) {
		try {
			if((con == null || con.isClosed()) && database_server != null) {
				connectToDatabase(database_server, db_name, db_user, db_password);
			}
		} catch (SQLException e1) {			
			e1.printStackTrace();
		}
		if(firstDelete) {
			if(deleteRecommendationData == null) {
				try {
					deleteRecommendationData = con.prepareStatement("DELETE FROM "+userRecommendationTable+" WHERE "+
							userIdColumn+"=?");
				} catch (SQLException e) {					
					e.printStackTrace();
				}
			}
			try {
				deleteRecommendationData.setLong(1,userID);
				int erasedRows = deleteRecommendationData.executeUpdate();
				System.out.println("Lineas borradas del usuario "+userID+" en tabla "+userRecommendationTable+": "+erasedRows);
			} catch (SQLException e1) {				
				e1.printStackTrace();
			}
		}
		if(insertRecommendationData == null) {
			try {
				insertRecommendationData = con.prepareStatement("INSERT INTO "+userRecommendationTable+"("+userIdColumn+","
						+messageIdColumn+","+userAfinityColumn+") values (?,?,?)");
			} catch (SQLException e1) {				
				e1.printStackTrace();
			}
		}
		try {
			insertRecommendationData.setLong(1,userID);
			int addedRows = 0;
			for(Long messageID : recommendationUserData.keySet()) {			
				//String query = initialQuery + messageID + "," + recommendationUserData.get(messageID) + ")";
				insertRecommendationData.setLong(2,messageID);
				insertRecommendationData.setFloat(3,recommendationUserData.get(messageID));
				addedRows += insertRecommendationData.executeUpdate();
			}
			System.out.println("Lineas añadidas al usuario "+userID+" en tabla "+userRecommendationTable+": "+addedRows);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void updateRecommendationUserData (boolean firstDelate, long userID, HashMap<Long, Float> recommendationUserData,
			String userRecommendationTable, String userIdColumn, String messageIdColumn, String userAfinityColumn, 
			boolean regenatePreparedStatement) {
		try {
			deleteRecommendationData = con.prepareStatement("DELETE FROM "+userRecommendationTable+" WHERE "+
					userIdColumn+"=?");
			insertRecommendationData = con.prepareStatement("INSERT INTO "+userRecommendationTable+"("+userIdColumn+","
					+messageIdColumn+","+userAfinityColumn+") values (?,?,?)");
		} catch (SQLException e) {					
			e.printStackTrace();
		}
		updateRecommendationUserData (firstDelate, userID, recommendationUserData,
				userRecommendationTable, userIdColumn, messageIdColumn, userAfinityColumn);
	}
		
		

}
