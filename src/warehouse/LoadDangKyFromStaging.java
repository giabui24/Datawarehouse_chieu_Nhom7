package warehouse;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class LoadDangKyFromStaging {
	ArrayList<DangKy> listDK = new ArrayList<>();

	public void loadDK(int idConfig) throws SQLException {
		ConnectDatabase conDB = new ConnectDatabase();
		ResultSet staging = null;
		DangKy dk;
		try {
			staging = conDB.loadStagingDB(idConfig);
		} catch (Exception e) {
			System.out.println(e.toString());
		}
		if (staging != null) {
			while (staging.next()) {
				dk = new DangKy(staging.getString(2), staging.getString(3), staging.getString(4), staging.getString(5), staging.getString(6));
				listDK.add(dk);
			}
		}
	}

	public ArrayList<DangKy> getStagingDK(int idConfig) throws SQLException {
		loadDK(idConfig);
		return listDK;
	}

	public static void main(String[] args) throws SQLException {
		LoadDangKyFromStaging load = new LoadDangKyFromStaging();
		ArrayList<DangKy> List = load.getStagingDK(6);
		for (DangKy dangKy : List) {
			System.out.println(dangKy.toString());
		}
	}
}
