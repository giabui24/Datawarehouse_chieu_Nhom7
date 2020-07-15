package connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ConnectMysql {
	private static String DB_URL = "jdbc:mysql://localhost:3306/wh_update";
	private static String USER_NAME = "root";
	private static String PASSWORD = "root";

	public static Connection getConnection() {
//		 String DB_URL = "jdbc:mysql://localhost:3306/warehouse";
//		 String USER_NAME = "root";
//		 String PASSWORD = "root";

		Connection conn = null;
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			conn = DriverManager.getConnection(DB_URL, USER_NAME, PASSWORD);
			System.out.println("connect successfully!");
		} catch (Exception ex) {
			System.out.println("connect failure!");
			ex.printStackTrace();
		}
		return conn;
	}
	
	// thuc thi lenh sql
		public void perform(String sql) throws Exception {
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			stmt.executeUpdate(sql);
			stmt.close();
		}
		
		// ch�?n dữ liệu sql
		public ResultSet selectDatabase(String sql) throws Exception {
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			return rs;
		}

		public static void main(String[] args) {
			Connection connection = ConnectMysql.getConnection();
			
		}
}
