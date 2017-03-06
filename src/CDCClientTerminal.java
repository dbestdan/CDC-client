import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class CDCClientTerminal implements Runnable {
	
	private String databaseUrl ="";
	private String userName ="";
	private String password ="";
	private String driver ="";
	private String latestRecordCsvFile ="";
	private Long latestRecordIdFromCsv;
	private Long latestRecordIdFromDatabase;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		new CDCClientTerminal();
		
	}
	
	public CDCClientTerminal(){
		Properties prop = new Properties();
		try{
			prop.load(new FileInputStream(System.getProperty("prop")));
		} catch (IOException e) {
			System.out.println("Couldn't load property file");
		}
		
		databaseUrl = prop.getProperty("databaseUrl");
		userName = prop.getProperty("userName");
		password = prop.getProperty("password");
		driver = prop.getProperty("driver");
		latestRecordCsvFile = prop.getProperty("latestRecordCsvFile");
		try {
			Connection conn = CDCConnection.getConnection(databaseUrl, driver, userName, password);
			if(conn!=null){
				System.out.println("connection successful");
			}
			latestRecordIdFromCsv = getLatestRecordIdFromCsv(latestRecordCsvFile);
			latestRecordIdFromDatabase = CDCConnection.getLatestRecordIdFromDatabase(conn);
			System.out.println("csv: "+ latestRecordIdFromCsv);
			System.out.println("database: " + latestRecordIdFromCsv);
		} catch (SQLException se){
			System.out.println("Couldn't connect to the database");
			se.printStackTrace();
		} catch (ClassNotFoundException cnfe){
			System.out.println("Unable to load database driver");
			cnfe.printStackTrace();
		}
		
		

	}
	
	public void run(){
		try {
			Connection conn = CDCConnection.getConnection(databaseUrl, driver, userName, password);
	
		}catch (SQLException se){
			System.out.println("Couldn't connect to the database");
			se.printStackTrace();
		}catch (ClassNotFoundException cnfe){
			System.out.println("Unable to load database driver");
			cnfe.printStackTrace();
		}
	}
	
	
	/**
	 * Function to return latest id of the record 
	 * migrated to data ware house
	 * 
	 */

	public Long getLatestRecordIdFromCsv(String fileUrl){
        String line = "";
        String cvsSplitBy = ",";
        Long recordId = new Long(0);

        try (BufferedReader br = new BufferedReader(new FileReader(latestRecordCsvFile))) {

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
}
