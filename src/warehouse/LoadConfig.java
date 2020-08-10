package warehouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class LoadConfig {
	ArrayList<Config> listConfig = new ArrayList<>();

	public ResultSet loadDBConfig() throws SQLException, ClassNotFoundException {
		Connection connection;
		ResultSet result = null;
		String sql = "";
		Class.forName("com.mysql.jdbc.Driver");
		String url = "jdbc:mysql://localhost:3306/wh_update?useUnicode=true&amp;characterEncoding=utf8?autoReconnect=true&useSSL=false";
		String user = "root";
		String password = "root";
		connection = DriverManager.getConnection(url, user, password);
		sql = "Select * from Config";
		result = connection.createStatement().executeQuery(sql);
		return result;
	}
	public ArrayList<Config> loadConfig() throws SQLException {
		Config config = null;
		ResultSet ReConfig = null;
		try {
			ReConfig = loadDBConfig();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		while (ReConfig.next()) {
			String idConfig = ReConfig.getString("idConfig");
			String hostName = ReConfig.getString("hostName");
			String port = ReConfig.getString("port");
			String user = ReConfig.getString("user");
			String password = ReConfig.getString("password");
			String remotepath = ReConfig.getString("remotepath");
			String namesub = ReConfig.getString("namesub");
			String stagingload = ReConfig.getString("stagingload");
			String sVNameSta = ReConfig.getString("sVNameSta");
			String dBNameSta = ReConfig.getString("dBNameSta");
			String tBNameSta = ReConfig.getString("tBNameSta");
			String userNameSta = ReConfig.getString("userNameSta");
			String passSta = ReConfig.getString("passSta");
			String sVNameWH = ReConfig.getString("sVNameWH");
			String dBNameWH = ReConfig.getString("dBNameWH");
			String tBNameWH = ReConfig.getString("tBNameWH");
			String userNameWH = ReConfig.getString("userNameWH");
			String passWH = ReConfig.getString("passWH");
			config = new Config(idConfig, hostName, port, user, password, remotepath, namesub,stagingload, sVNameSta, dBNameSta,
					tBNameSta, userNameSta, passSta, sVNameWH, dBNameWH, tBNameWH, userNameWH, passWH);
			listConfig.add(config);
		}
		return listConfig;
	}

	public Config getConfig(int idConfig) throws SQLException {
		loadConfig();
		return listConfig.get(idConfig - 1);
	}

	public static void main(String[] args) throws SQLException {
		LoadConfig lo = new LoadConfig();
		Config co = lo.getConfig(2);
		System.out.println(co.toString());
	}
}
