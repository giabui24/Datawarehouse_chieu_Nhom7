package warehouse;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class LoadSinhVienFromStaging {
ArrayList<SinhVien> listSV = new ArrayList<>();
	
	
	public void loadSV(int idConfig) throws SQLException {
		ConnectDatabase conDB = new ConnectDatabase();
		ResultSet staging = null;
		SinhVien sv;
		try {
			staging = conDB.loadStagingDB(idConfig);
		}
		catch (Exception e) {
			// TODO: handle exception
		}
		if (staging != null) {
			while (staging.next()) {
				sv = new SinhVien(staging.getString(2), staging.getString(3), staging.getString(4), staging.getString(5), staging.getString(6), staging.getString(7),
						 staging.getString(8), staging.getString(9), staging.getString(10), staging.getString(11));
				listSV.add(sv);
			}
		}
	}
	public ArrayList<SinhVien> getStagingSV(int idConfig) throws SQLException{
		loadSV(idConfig);
		return listSV;
	}
	
	public static void main(String[] args) throws SQLException {
		LoadSinhVienFromStaging load=new LoadSinhVienFromStaging();
		ArrayList<SinhVien> list= load.getStagingSV(1);
		for (SinhVien sv : list) {
			System.out.println(sv.toString());
			
		}
	}

}
