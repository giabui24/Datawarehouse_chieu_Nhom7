package warehouse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ConnectDatabase {
	public ResultSet loadDBConfig() throws SQLException, ClassNotFoundException {
		Connection connection;
		ResultSet result = null;
		String sql = "";
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String url = "jdbc:mysql://localhost:3306/datawarehouse?useUnicode=true&amp;characterEncoding=utf8?autoReconnect=true&useSSL=false";
			String user = "root";
			String password = "0411";
			connection = DriverManager.getConnection(url, user, password);
			sql = "Select * from Config where id=1";
			result = connection.createStatement().executeQuery(sql);
		} catch (ClassNotFoundException | SQLException e) {
			System.out.println("Kết nối thất bại");
		}
		return result;
	}

	public Connection connectDateDim() throws SQLException, ClassNotFoundException {
		Connection connection = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String url = "jdbc:mysql://localhost:3306/datawarehouse?useUnicode=true&amp;characterEncoding=utf8?autoReconnect=true&useSSL=false";
			String user = "root";
			String password = "0411";
			connection = DriverManager.getConnection(url, user, password);
		} catch (ClassNotFoundException | SQLException e) {
			System.out.println("Kết nối thất bại");
		}
		return connection;
	}
	public ResultSet loadSDB() throws SQLException, ClassNotFoundException {
		String sql = "";
		Connection connection;
		ResultSet result = null;
		try {
			ResultSet re = this.loadDBConfig();
			re.next();
			String serverName = re.getString(2);
			String databaseName = re.getString(3);
			String sourceTB = re.getString(4);
			String user = re.getString(5);
			String pass = re.getString(6);
			sql = "Select * from " + sourceTB;
			Class.forName("com.mysql.jdbc.Driver");
			String connectionURL = "jdbc:mysql://" + serverName + "/" + databaseName
					+ "?useUnicode=true&amp;characterEncoding=utf8?autoReconnect=true&useSSL=false";
			connection = DriverManager.getConnection(connectionURL, user, pass);
			result = connection.createStatement().executeQuery(sql);
		} catch (ClassNotFoundException e) {
			System.out.println("Kết nối thất bại");
		}
		return result;
	}

	public Connection connectDDB() throws SQLException, ClassNotFoundException {
		Connection con = null;
		ConnectDatabase conDB = new ConnectDatabase();
		ResultSet re = conDB.loadDBConfig();
		re.next();
		String serverName = re.getString(7);
		String databaseName = re.getString(8);
		String user = re.getString(10);
		String pass = re.getString(11);
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String connectionURL = "jdbc:mysql://" + serverName + "/" + databaseName
					+ "?useUnicode=true&amp;characterEncoding=utf8?autoReconnect=true&useSSL=false";
			con = DriverManager.getConnection(connectionURL, user, pass);
		} catch (ClassNotFoundException e) {
			System.out.println("Kết nối thất bại");
			e.printStackTrace();
		}
		return con;
	}

	public Connection connectLog() throws SQLException, ClassNotFoundException {
		Connection con = null;
		ConnectDatabase conDB = new ConnectDatabase();
		ResultSet re = conDB.loadDBConfig();
		re.next();
		String serverName = re.getString(7);
		String databaseName = re.getString(8);
		String user = re.getString(10);
		String pass = re.getString(11);
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String connectionURL = "jdbc:mysql://" + serverName + "/" + databaseName
					+ "?useUnicode=true&amp;characterEncoding=utf8?autoReconnect=true&useSSL=false";
			con = DriverManager.getConnection(connectionURL, user, pass);
		} catch (ClassNotFoundException e) {
			System.out.println("Kết nối thất bại");
			e.printStackTrace();
		}
		return con;
	}

	/*
	 * public ResultSet loadDBConfig() throws SQLException{ Connection
	 * connection = null; String
	 * sql="Select * from Config where sourceTB='Staging2'"; Statement sta =
	 * null; try {
	 * Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"); String
	 * connectionURL =
	 * "jdbc:sqlserver://DESKTOP-7P5LFFB\\SQLEXPRESS:1433;databaseName=DataWarehouse;user=sa;password=0411";
	 * connection = DriverManager.getConnection(connectionURL);
	 * sta=connection.createStatement(); } catch (ClassNotFoundException e) {
	 * System.out.println("Kết nối thất bại"); e.printStackTrace(); } return
	 * sta.executeQuery(sql); }
	 * 
	 * public ResultSet loadSDB() throws SQLException{ ConnectDatabase conDB =
	 * new ConnectDatabase(); ResultSet re=conDB.loadDBConfig(); re.next();
	 * String serverName=re.getString(2); String databaseName=re.getString(3);
	 * String sourceTB=re.getString(4); String user=re.getString(5); String
	 * pass=re.getString(6); Connection connection = null; Statement sta = null;
	 * String sql = "Select * from "+sourceTB; try {
	 * Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"); String
	 * connectionURL =
	 * "jdbc:sqlserver://"+serverName+";databaseName="+databaseName+";user="+
	 * user+";password="+pass; connection =
	 * DriverManager.getConnection(connectionURL); sta =
	 * connection.createStatement(); } catch (ClassNotFoundException e) {
	 * System.out.println("Kết nối thất bại"); e.printStackTrace(); } return
	 * sta.executeQuery(sql); } public Connection connectDDB() throws
	 * SQLException{ ConnectDatabase conDB = new ConnectDatabase(); ResultSet
	 * re=conDB.loadDBConfig(); re.next(); String serverName=re.getString(7);
	 * String databaseName=re.getString(8); String user=re.getString(10); String
	 * pass=re.getString(11); Connection connection=null; try {
	 * Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"); String
	 * connectionURL =
	 * "jdbc:sqlserver://"+serverName+";databaseName="+databaseName+";user="+
	 * user+";password="+pass; connection =
	 * DriverManager.getConnection(connectionURL); } catch
	 * (ClassNotFoundException e) { System.out.println("Kết nối thất bại");
	 * e.printStackTrace(); } return connection; } public Connection
	 * connectLog() throws SQLException{ ConnectDatabase conDB = new
	 * ConnectDatabase(); ResultSet re=conDB.loadDBConfig(); re.next(); String
	 * serverName=re.getString(7); String databaseName=re.getString(8); String
	 * user=re.getString(10); String pass=re.getString(11); Connection
	 * connection=null; try {
	 * Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"); String
	 * connectionURL =
	 * "jdbc:sqlserver://"+serverName+";databaseName="+databaseName+";user="+
	 * user+";password="+pass; connection =
	 * DriverManager.getConnection(connectionURL); } catch
	 * (ClassNotFoundException e) { System.out.println("Kết nối thất bại");
	 * e.printStackTrace(); } return connection; }
	 */
}
