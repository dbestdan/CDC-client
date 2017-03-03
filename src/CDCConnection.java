import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
}
