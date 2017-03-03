import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class CDCConnection {

	public static Connection getConnection(String serverUrl, String userName, String password) throws SQLException {
		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", userName);
		connectionProps.put("password", password);
		conn = DriverManager.getConnection(serverUrl,connectionProps);
		return conn;
				
	}
}
