import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class CDCConnection {

	public static Connection getConnection(String databaseUrl, String driver, String userName, String password)
			throws SQLException, ClassNotFoundException {
		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", userName);
		connectionProps.put("password", password);
		Class.forName(driver);
		conn = DriverManager.getConnection(databaseUrl, connectionProps);
		return conn;

	}

	/**
	 * Function to return the latest id of the changed record present in the
	 * database
	 * 
	 * @param conn
	 * @return latestIdFromDatabase
	 */
	public static Long getLatestRecordIdFromDatabase(Connection conn) {
		Long latestIdFromDatabase = new Long(0);
		try {
			Statement stmt = conn.createStatement();
			String query = "SELECT MAX(event_id) FROM audit.logged_actions";
			ResultSet rs = stmt.executeQuery(query);
			if (rs.next()) {
				latestIdFromDatabase = rs.getLong(1);
			}
		} catch (SQLException sqle) {
			System.out.println("Couldn't get latest record id from the database");
			sqle.printStackTrace();
		}
		return latestIdFromDatabase;
	}

	/**
	 * Function to read csv_file which contains only one value
	 * 
	 */

	public static Long getValueFromCsv(String fileUrl) {
		String line = "";
		String cvsSplitBy = ",";
		Long recordId = new Long(0);

		try {
			BufferedReader br = new BufferedReader(new FileReader(fileUrl));

			while ((line = br.readLine()) != null && !line.isEmpty()) {

				// use comma as separator
				recordId = Long.parseLong(line.trim());

			}
			br.close();

		} catch (IOException e) {
			System.out.println("Couldn't read CSV file");
			e.printStackTrace();
		}
		return recordId;
	}

	/**
	 * Function to write a single value to a csv_file
	 * 
	 */

	public static void setValueToCsv(String fileUrl, String value) {
		try {
			File csvFile = new File(fileUrl);
			csvFile.createNewFile(); 
			PrintWriter csvFileWriter = new PrintWriter(csvFile);
			csvFileWriter.print(value);
			csvFileWriter.close();

		} catch (IOException e) {
			System.out.println("Couldn't write to CSV file");
			e.printStackTrace();
		}

	}

	/**
	 * function to get the records from audit.logged_actions table within the
	 * range given by the user
	 * 
	 * @param conn
	 * @param beginEventId
	 * @param endEventId
	 * @return
	 */
	public static ResultSet getChangedDataInGivenRange(Connection conn, Long beginEventId, Long endEventId) {
		ResultSet rs = null;
		try {
			String query = "SELECT * FROM audit.logged_actions " + "WHERE event_id BETWEEN ? and ? ORDER BY event_id";
			PreparedStatement pstmt = conn.prepareStatement(query);
			pstmt.setLong(1, beginEventId);
			pstmt.setLong(2, endEventId);
			rs = pstmt.executeQuery();
		} catch (SQLException sqle) {
			System.out.println("Couldn't get changed data in given range");
			sqle.printStackTrace();
		}
		return rs;

	}
}
