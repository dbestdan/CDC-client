import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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
		}catch(SQLException sqle){
			System.out.println("Couldn't get latest record id from the database");
			sqle.printStackTrace();
		}
		return latestIdFromDatabase;
	}
	
	
	/**
	 * Function to return latest id of the record 
	 * migrated to data ware house
	 * 
	 */

	public static Long getLatestRecordIdFromCsv(String fileUrl){
        String line = "";
        String cvsSplitBy = ",";
        Long recordId = new Long(0);

        try (BufferedReader br = new BufferedReader(new FileReader(fileUrl))) {

            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] changedRecord = line.split(cvsSplitBy);
                recordId = new Long(changedRecord[1]);               

            }

        } catch (IOException e) {
        	System.out.println("Couldn't read CSV file");
            e.printStackTrace();
        }
        return recordId;
	}
	
	
	/**
	 * function to get the records from audit.logged_actions table 
	 * within the range given by the user
	 * @param conn
	 * @param beginEventId
	 * @param endEventId
	 * @return
	 */
	public static ResultSet getChangedDataInGivenRange(Connection conn, Long beginEventId, Long endEventId){
		ResultSet rs = null;
		try{
			String query = "SELECT * FROM audit.logged_actions "
							+ "WHERE event_id BETWEEN ? and ? ORDER BY event_id";
			PreparedStatement pstmt = conn.prepareStatement(query);
			pstmt.setLong(1, beginEventId);
			pstmt.setLong(2, endEventId);
			rs = pstmt.executeQuery();
		}catch(SQLException sqle){
			System.out.println("Couldn't get changed data in given range");
			sqle.printStackTrace();
		}
		return rs;
		
	}
}
