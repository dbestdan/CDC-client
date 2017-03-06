import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class CDCConnection {

	public static Connection getConnection(String databaseUrl, String driver,String userName, String password) throws SQLException, ClassNotFoundException {
		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", userName);
		connectionProps.put("password", password);
		Class.forName(driver);
		conn = DriverManager.getConnection(databaseUrl,connectionProps);
		return conn;
				
	}
	
	/**
	 * Function to return the latest id of 
	 * the changed record present in the database 
	 * @param conn
	 * @return latestIdFromDatabase
	 */
	public static Long getLatestRecordIdFromDatabase(Connection conn){
		Long latestIdFromDatabase = new Long(0);
		try{
			Statement stmt = conn.createStatement();
			String query = "SELECT MAX(event_id) FROM audit.logged_actions";
			ResultSet rs = stmt.executeQuery(query);
			if(rs.next()){
				latestIdFromDatabase = rs.getLong(1);
			}
		}catch(Exception sqle){
			System.out.println("Couldn't get latest record id from the database");
			sqle.printStackTrace();
		}
		return latestIdFromDatabase;
	}
}
