package tool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import connection.GetConnection;

public class TruncateTable {
	public static void truncateTable( String tbName) {
		Connection conn = null;
		PreparedStatement pre_control = null;
		String truncateSql = "TRUNCATE TABLE " + tbName;
		try {
			conn = new GetConnection().getConnection("staging?serverTimezone=UTC");
					pre_control = conn.prepareStatement(truncateSql);
			pre_control.executeUpdate();
			conn.close();	
			System.out.println("Truncate thanh cong "+ tbName);
		} catch (SQLException e) {
			System.out.println("<---> ERROR [Truncate table] [database: table: " + tbName + "]: " + e.getMessage());
		}	
	}
}
