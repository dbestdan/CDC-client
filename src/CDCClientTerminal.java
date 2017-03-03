import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class CDCClientTerminal {
	
	private String databaseUrl ="";
	private String userName ="";
	private String password ="";
	private String driver ="";

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
		try {
			Connection conn = CDCConnection.getConnection(databaseUrl, driver, userName, password);
			if(conn!=null){
				System.out.println("connection successful");
			}
		} catch (SQLException se){
			System.out.println("Couldn't connect to the database");
			se.printStackTrace();
		} catch (ClassNotFoundException cnfe){
			System.out.println("Unable to load database driver");
			cnfe.printStackTrace();
		}
	}

}
