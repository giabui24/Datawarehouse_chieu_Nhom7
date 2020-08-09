package connection;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class GetConnection {
      //test_________######______________git
	String driver = null;
	String url = null;
	String user = null;
	String pass = null;
	String databasebName = null;

	public Connection getConnection(String location) {
		String link = "C:\\Users\\ASUS\\git\\Datawarehouse_chieu_Nhom7\\src\\config\\config.properties";
		Connection result = null;
		
		if (location.equalsIgnoreCase("databasecontrol?serverTimezone=UTC")) {
			try (InputStream input = new FileInputStream(link)) {
				Properties prop = new Properties();
				prop.load(input);
				driver = prop.getProperty("dricer_server");
				url = prop.getProperty("url_server");
				databasebName = prop.getProperty("dbName_control");
				user = prop.getProperty("userName_server");
				pass = prop.getProperty("password_server");
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		} else if (location.equalsIgnoreCase("mart")) {
			try (InputStream input = new FileInputStream(link)) {
				Properties prop = new Properties();
				prop.load(input);
				driver = prop.getProperty("dricer_server");
				url = prop.getProperty("url_server");
				databasebName = prop.getProperty("dbName_datamart");
				user = prop.getProperty("userName_server");
				pass = prop.getProperty("password_server");
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		} else if (location.equalsIgnoreCase("staging?serverTimezone=UTC")) {
			try (InputStream input = new FileInputStream(link)) {
				Properties prop = new Properties();
				prop.load(input);
				driver = prop.getProperty("driver_local");
				url = prop.getProperty("url_local");
				databasebName = prop.getProperty("dbName_staging");
				user = prop.getProperty("user_local");
				pass = prop.getProperty("pass_local");
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		} else if (location.equalsIgnoreCase("warehouse")) {
			try (InputStream input = new FileInputStream(link)) {
				Properties prop = new Properties();
				prop.load(input);
				driver = prop.getProperty("driver_local");
				url = prop.getProperty("url_local");
				databasebName = prop.getProperty("dName_datawarehouse");
				user = prop.getProperty("user_local");
				pass = prop.getProperty("pass_local");
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		} else {
			System.out.println(driver);

		}
		try {
			Class.forName(driver);
			String connectionURL = url + databasebName;
			try {
				result = DriverManager.getConnection(connectionURL, user, pass);
			} catch (SQLException e) {
				System.out.println(e);
				System.exit(0);
				e.printStackTrace();
			}

		} catch (ClassNotFoundException e) {
			System.out.println("Khong thay file config");
			System.exit(0);
			e.printStackTrace();
		}

		return result;
	}

	public static void main(String[] args) {
		Connection conn = new GetConnection().getConnection("staging?serverTimezone=UTC");
		if (conn != null) {
			System.out.println("Thanh cong");

		}
	}
}
