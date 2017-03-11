import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

// To do for the name of the csv file  
// use another document to read a id

public class CDCClient implements CDCClientConfig {

	private long latestRecordIdFromCsv;
	private long latestRecordIdFromDatabase;

	private long totalChangedData;

	private long totalChangedDataPerTerminal;

	private long currentFileIdFromCsv;
	private long terminalsStarted = 0;
	private long sessonSleepTimeMilli = 0;

	private CDCClientTerminal[] cDCTerminals;
	private boolean isSessionRunning;

	long programStartTimeMilli = 0;
	long sessionStartTime = 0;
	long sessionEndTime = 0;
	long programEndTimeMilli = 0;

	int sessionCount = 0;

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
		String pCurrentFileIdCsvFileUrl = prop.getProperty("currentFileIdCsvFileUrs");
		String pNumberOfTerminals = prop.getProperty("numberOfTerminals");
		String pChangedDataRecordFileUrl = prop.getProperty("changedDataRecordFileUrl");
		String pNumberOfRecordsPerIteration = prop.getProperty("numberOfRecordsPerIteration");
		String pFirstSessionStartTimeMin = prop.getProperty("firstSessionStartTimeMin");
		String pSessionSleepTimeMin = prop.getProperty("sessionSleepTimeMin");
		String pProgramRunTimeMin = prop.getProperty("runMins");
		long firstSessionStartTimeMilli = 0;

		long programRunTimeMilli = 0;

		int numberOfTerminals = 0;
		int numberOfRecordsPerIteration = 0;

		try {
			numberOfTerminals = Integer.parseInt(pNumberOfTerminals);
			if (numberOfTerminals == 0) {
				throw new NumberFormatException();
			}
			
		} catch (NumberFormatException nfe) {
			System.out.println("Number of Terminal is not valid. It should be more than 0");
			nfe.printStackTrace();
		}

		try {
			numberOfRecordsPerIteration = Integer.parseInt(pNumberOfRecordsPerIteration);
			if (numberOfRecordsPerIteration == 0) {
				throw new NumberFormatException();
			}
		} catch (NumberFormatException nfe) {
			System.out.println("Number of records per iteration is not valid. It should be more than 0");
		}

		try {
			firstSessionStartTimeMilli = Long.parseLong(pFirstSessionStartTimeMin.trim()) * 60000;
			sessonSleepTimeMilli = Long.parseLong(pSessionSleepTimeMin.trim()) * 60000;
			programRunTimeMilli = Long.parseLong(pProgramRunTimeMin.trim()) * 60000;
		} catch (NumberFormatException nfe) {
			System.out.println("Session Time number format exception");
			nfe.printStackTrace();
		}

		programStartTimeMilli = System.currentTimeMillis();
		programEndTimeMilli = programStartTimeMilli + programRunTimeMilli;

		System.out.println("Program StartTime " + dateFormat.format(new java.util.Date()));

		// start only after firstSessionStartTime is complete
		try {
			System.out.println("Session will start after " + pFirstSessionStartTimeMin + " min");
			Thread.sleep(firstSessionStartTimeMilli);
		} catch (InterruptedException ie) {
			System.out.println("Problem in first session start time");
			ie.printStackTrace();
		}

		while (programEndTimeMilli > System.currentTimeMillis()) {
			sessionCount++;
			terminalsStarted = numberOfTerminals;
			System.out.println("Parent Started");
			System.out.println("Session Started: " + sessionCount + " Session Start Time: "
					+ dateFormat.format(new java.util.Date()));

			try {
				Connection conn = CDCConnection.getConnection(pDatabaseUrl, pDriver, pUserName, pPassword);
				latestRecordIdFromCsv = CDCConnection.getValueFromCsv(pLatestRecordCsvFileUrl);
				latestRecordIdFromDatabase = CDCConnection.getLatestRecordIdFromDatabase(conn);
				currentFileIdFromCsv = CDCConnection.getValueFromCsv(pCurrentFileIdCsvFileUrl);
				conn.close();

				System.out.println("Id in csv: " + latestRecordIdFromCsv);
				System.out.println("Latest Id in database: " + latestRecordIdFromDatabase);
			} catch (SQLException se) {
				System.out.println("Couldn't connect to the database");
				se.printStackTrace();
			} catch (ClassNotFoundException cnfe) {
				System.out.println("Unable to load database driver");
				cnfe.printStackTrace();
			}

			/**
			 * Sleep if database is empty or if there is no new change data from
			 * last capture
			 */
			if (latestRecordIdFromDatabase == 0) {
				System.out.println("No record in the database");
				System.out.println("Session Ended: " + sessionCount);
				System.out.println("Next Session will start after " + pFirstSessionStartTimeMin + " min");
				try {
					Thread.sleep(sessonSleepTimeMilli);
				} catch (InterruptedException ie) {
					System.out.println("Problem in session sleep");
					ie.printStackTrace();
				}
				continue;
			}
			if (latestRecordIdFromDatabase == latestRecordIdFromCsv) {
				System.out.println("No new changed record present");
				System.out.println("Session Ended: " + sessionCount);
				System.out.println("Next Session will start after " + pFirstSessionStartTimeMin + " min");
				try {
					Thread.sleep(sessonSleepTimeMilli);
				} catch (InterruptedException ie) {
					System.out.println("Problem in session sleep");
					ie.printStackTrace();
				}
				continue;
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
				if (i == (numberOfTerminals - 1)) {
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

				currentFileIdFromCsv++;
				String changedDataRecordFileUrl = pChangedDataRecordFileUrl + currentFileIdFromCsv + ".csv";
				CDCClientTerminal cDCTerminal = new CDCClientTerminal(connTerminal, initialCursorPoint,
						finalCursorPoint, numberOfRecordsPerIteration, changedDataRecordFileUrl, this);
				cDCTerminals[i] = cDCTerminal;

				initialCursorPoint = finalCursorPoint;
			}

			synchronized (cDCTerminals) {
				System.out.println("Starting all terminals...");
				for (int i = 0; i < numberOfTerminals; i++) {
					(new Thread(cDCTerminals[i])).start();

				}

			}

			// update value of current_file_id and latest_record
			CDCConnection.setValueToCsv(pLatestRecordCsvFileUrl, Long.toString(latestRecordIdFromDatabase));
			CDCConnection.setValueToCsv(pCurrentFileIdCsvFileUrl, Long.toString(currentFileIdFromCsv));

			System.out.println("All terminals started executing");
			
			try {
				System.out.println("Parent will sleep for "+ pSessionSleepTimeMin + " mins");
				Thread.sleep(sessonSleepTimeMilli);
			} catch (InterruptedException ie) {
				System.out.println("Problem in session sleep");
				ie.printStackTrace();
			}
		}
		System.out.println("Program ended: " + dateFormat.format(new java.util.Date()));

	}

	public void signalTerminalEnded(CDCClientTerminal terminal) {
		synchronized (cDCTerminals) {
			boolean found = false;
			terminalsStarted--;
			for (int i = 0; i < cDCTerminals.length && !found; i++) {
				if (cDCTerminals[i] == terminal) {
					cDCTerminals[i] = null;
					found = true;
					System.out.println("Terminal ended: " + i);
				}
			}

		}

		if (terminalsStarted == 0) {
			System.out.println(
					"Session Ended: " + sessionCount + " Session end time: " + dateFormat.format(new java.util.Date()));
			
//			try {
//				Thread.sleep(sessonSleepTimeMilli);
//			} catch (InterruptedException ie) {
//				System.out.println("Problem in session sleep");
//				ie.printStackTrace();
//			}
		}
	}

}
