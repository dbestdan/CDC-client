import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class CDCClientTerminal implements Runnable {
	Connection conn = null;
	Long initialCursorPoint = null;
	Long finalCursorPoint = null;
	Long currentCursorPoint = null;
	Long nextCursorPoint = null;
	int numberOfRecordsPerIteration = 0;
	String changedDataRecordFileUrl = null;
	File changedDataRecordFileCsv = null;
	PrintWriter changedDataRecordFileWriter = null;
	ResultSetMetaData resultSetMetaData = null;
	Statement stmt = null;

	public CDCClientTerminal(Connection conn, Long initialCursorPoint, Long finalCursorPoint,
			int numberOfRecordsPerIteration, String changedDataRecordFileUrl) {
		this.conn = conn;
		this.initialCursorPoint = initialCursorPoint;
		this.finalCursorPoint = finalCursorPoint;
		this.numberOfRecordsPerIteration = numberOfRecordsPerIteration;
		this.currentCursorPoint = initialCursorPoint;
		this.changedDataRecordFileUrl = changedDataRecordFileUrl;
	}

	public void run() {

		// create a file for storing the data
		try {
			changedDataRecordFileCsv = new File(changedDataRecordFileUrl);
			changedDataRecordFileCsv.createNewFile(); // if already exists
														// it will do
														// nothing
			changedDataRecordFileWriter = new PrintWriter(changedDataRecordFileCsv);

		} catch (IOException ioe) {
			System.out.println("Cannot create change data record file: " + changedDataRecordFileUrl);
			ioe.printStackTrace();

		}

		// stop thread if it is end of the record
		while (currentCursorPoint != finalCursorPoint) {
			currentCursorPoint = currentCursorPoint + 1;
			nextCursorPoint = currentCursorPoint + numberOfRecordsPerIteration;

			if (finalCursorPoint < nextCursorPoint) {
				nextCursorPoint = finalCursorPoint;
			}

			ResultSet rs = CDCConnection.getChangedDataInGivenRange(conn, currentCursorPoint, nextCursorPoint);
			try {
				resultSetMetaData = rs.getMetaData();
				int numberOfColumns = resultSetMetaData.getColumnCount();
				/*
				StringBuilder columnHeaderStringBuilder = new StringBuilder();
				for (int i = 1; i <= numberOfColumns; i++) {
					columnHeaderStringBuilder.append(resultSetMetaData.getColumnName(i));
					columnHeaderStringBuilder.append("§");
				}

				if (columnHeaderStringBuilder.length() > 0)
					columnHeaderStringBuilder.setLength(columnHeaderStringBuilder.length() - 1);

				changedDataRecordFileWriter.println(columnHeaderStringBuilder);
				*/

				while (rs.next()) {
					StringBuilder rowStringBuilder = new StringBuilder();
					for (int i = 1; i <= numberOfColumns; i++) {
						rowStringBuilder.append(rs.getString(i));
						rowStringBuilder.append("§");
					}
					changedDataRecordFileWriter.println(rowStringBuilder);
					if (rowStringBuilder.length() > 0)
						rowStringBuilder.setLength(rowStringBuilder.length() - 1);
				}

			} catch (SQLException sqle) {
				System.out.println("Problem in reading data from logged_actions table");
				sqle.printStackTrace();

			}

			currentCursorPoint = nextCursorPoint;
		}
		
		changedDataRecordFileWriter.close();

		System.out.println("end of terminal");

	}

}
