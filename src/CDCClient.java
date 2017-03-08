import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class CDCClient {

	private Long latestRecordIdFromCsv;
	private Long latestRecordIdFromDatabase;

	private Long totalChangedData;

	private Long totalChangedDataPerTerminal;

	private CDCClientTerminal[] cDCTerminals;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		new CDCClient();
	}

	public CDCClient() {
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(System.getProperty("prop")));
		} catch (IOException e) {
			System.out.println("Couldn't load property file");
		}

		String pDatabaseUrl = prop.getProperty("databaseUrl");
		String pUserName = prop.getProperty("userName");
		String pPassword = prop.getProperty("password");
		String pDriver = prop.getProperty("driver");
		String pLatestRecordCsvFileUrl = prop.getProperty("latestRecordCsvFileUrl");
		String pNumberOfTerminals = prop.getProperty("numberOfTerminals");
		String pChangedDataRecordFileUrl = prop.getProperty("changedDataRecordFileUrl");
		String pNumberOfRecordsPerIteration = prop.getProperty("numberOfRecordsPerIteration");
		int numberOfTerminals = 0;
		int numberOfRecordsPerIteration = 0;

		try {
			Connection conn = CDCConnection.getConnection(pDatabaseUrl, pDriver, pUserName, pPassword);
			if (conn != null) {
				System.out.println("connection successful");
			}
			latestRecordIdFromCsv = CDCConnection.getLatestRecordIdFromCsv(pLatestRecordCsvFileUrl);
			latestRecordIdFromDatabase = CDCConnection.getLatestRecordIdFromDatabase(conn);
			System.out.println("csv: " + latestRecordIdFromCsv);
			System.out.println("database: " + latestRecordIdFromDatabase);
		} catch (SQLException se) {
			System.out.println("Couldn't connect to the database");
			se.printStackTrace();
		} catch (ClassNotFoundException cnfe) {
			System.out.println("Unable to load database driver");
			cnfe.printStackTrace();
		}

		try {
			numberOfTerminals = Integer.parseInt(pNumberOfTerminals);
			if (numberOfTerminals == 0) {
				throw new NumberFormatException();
			}
		} catch (NumberFormatException nfe) {
			System.out.println("Number of Terminal is not valid. It should be more than 0");
		}

		try {
			numberOfRecordsPerIteration = Integer.parseInt(pNumberOfRecordsPerIteration);
			if (numberOfRecordsPerIteration == 0) {
				throw new NumberFormatException();
			}
		} catch (NumberFormatException nfe) {
			System.out.println("Number of records per iteration is not valid. It should be more than 0");
		}

		/**
		 * Terminate program if database is empty or if there is no new change
		 * data from last capture
		 */
		if (latestRecordIdFromDatabase == 0) {
			System.out.println("No record in the database");
			System.exit(0);
		} else if (latestRecordIdFromDatabase == latestRecordIdFromCsv) {
			System.out.println("No new changed record present");
			System.exit(0);
		}

		/**
		 * Calculate number of data, each terminal going read
		 */
		totalChangedData = latestRecordIdFromDatabase - latestRecordIdFromCsv;
		totalChangedDataPerTerminal = totalChangedData / numberOfTerminals;

		Long initialCursorPoint = latestRecordIdFromCsv;
		Long finalCursorPoint = new Long(0);
		cDCTerminals = new CDCClientTerminal[numberOfTerminals];
		for (int i = 0; i < numberOfTerminals; i++) {
			// check if it is the final terminal
			if (i == (numberOfTerminals-1)) {
				finalCursorPoint = latestRecordIdFromDatabase;
			} else
				finalCursorPoint = initialCursorPoint + totalChangedDataPerTerminal;

			Connection connTerminal = null;
			System.out.println("Creating database connection for terminal: " + i);

			try {
				connTerminal = CDCConnection.getConnection(pDatabaseUrl, pDriver, pUserName, pPassword);
			} catch (SQLException se) {
				System.out.println("Couldn't connect to the database");
				se.printStackTrace();
			} catch (ClassNotFoundException cnfe) {
				System.out.println("Unable to load database driver");
				cnfe.printStackTrace();
			}

			String changedDataRecordFileUrl = pChangedDataRecordFileUrl+ i + ".csv";
			CDCClientTerminal cDCTerminal = new CDCClientTerminal(connTerminal, initialCursorPoint, finalCursorPoint,
					numberOfRecordsPerIteration, changedDataRecordFileUrl);
			cDCTerminals[i] = cDCTerminal;

			initialCursorPoint = finalCursorPoint;
		}
		
		synchronized (cDCTerminals){
			System.out.println("Starting all terminals...");
			for(int i=0; i<numberOfTerminals; i++){
				(new Thread(cDCTerminals[i])).start();
			}
		}
		
		System.out.println("All terminals started executing");

	}

}
