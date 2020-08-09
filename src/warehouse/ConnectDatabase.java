import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ConnectDatabase {
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		ConnectDatabase connectDatabase = new ConnectDatabase();
		ResultSet SDB = connectDatabase.loadStagingDB(2);
		Connection DDB = connectDatabase.connectWarehouseDB(2);
		System.out.println(DDB);
		System.out.println(SDB);
	}

	public Connection connectDateDim() throws SQLException, ClassNotFoundException {
		Connection connection = null;
		Class.forName("com.mysql.jdbc.Driver");
		String url = "jdbc:mysql://localhost:3306/datawarehouse?useUnicode=true&amp;characterEncoding=utf8?autoReconnect=true&useSSL=false";
		String user = "root";
		String password = "0411";
		connection = DriverManager.getConnection(url, user, password);
		return connection;
	}

	public ResultSet loadStagingDB(int idConfig) throws SQLException, ClassNotFoundException {
		String sql = "";
		Connection connection;
		ResultSet result = null;
		Config config = new LoadConfig().getConfig(idConfig);
		String serverName = config.getsVNameSta();
		String databaseName = config.getdBNameSta();
		String sourceTB = config.gettBNameSta();
		String user = config.getUserNameSta();
		String pass = config.getPassSta();
		sql = "Select * from " + sourceTB;
		Class.forName("com.mysql.jdbc.Driver");
		String connectionURL = "jdbc:mysql://" + serverName + "/" + databaseName
				+ "?useUnicode=true&amp;characterEncoding=utf8?autoReconnect=true&useSSL=false";
		connection = DriverManager.getConnection(connectionURL, user, pass);
		result = connection.createStatement().executeQuery(sql);
		return result;
	}

	public Connection connectWarehouseDB(int idConfig) throws SQLException, ClassNotFoundException {
		Connection con = null;
		Config config = new LoadConfig().getConfig(idConfig);
		String serverName = config.getsVNameWH();
		String databaseName = config.getdBNameWH();
		String user = config.getUserNameWH();
		String pass = config.getPassWH();
		Class.forName("com.mysql.jdbc.Driver");
		String connectionURL = "jdbc:mysql://" + serverName + "/" + databaseName
				+ "?useUnicode=true&amp;characterEncoding=utf8?autoReconnect=true&useSSL=false";
		con = DriverManager.getConnection(connectionURL, user, pass);
		return con;
	}

	public Connection connectLog(int idConfig) throws SQLException, ClassNotFoundException {
		Connection con = null;
		Config config = new LoadConfig().getConfig(idConfig);
		String serverName = config.getsVNameWH();
		String databaseName = config.getdBNameWH();
		String user = config.getUserNameWH();
		String pass = config.getPassWH();
		Class.forName("com.mysql.jdbc.Driver");
		String connectionURL = "jdbc:mysql://" + serverName + "/" + databaseName
				+ "?useUnicode=true&amp;characterEncoding=utf8?autoReconnect=true&useSSL=false";
		con = DriverManager.getConnection(connectionURL, user, pass);
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
