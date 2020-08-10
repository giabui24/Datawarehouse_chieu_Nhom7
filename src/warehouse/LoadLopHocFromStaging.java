package warehouse;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class LoadLopHocFromStaging {
ArrayList<LopHoc> listLH = new ArrayList<>();
	
	public void loadLH(int idConfig) throws SQLException {
		ConnectDatabase conDB = new ConnectDatabase();
		ResultSet staging = null;
		LopHoc lh;
		boolean check = true;
		try {
			staging = conDB.loadStagingDB(idConfig);
		}
		catch (Exception e) {
			// TODO: handle exception
		}
		if (staging != null) {
			while (staging.next() && check) {
				lh = new LopHoc(staging.getString(2), staging.getString(3), staging.getString(4), staging.getString(5));
				listLH.add(lh);
			}
		}
	}
	public ArrayList<LopHoc> getStagingLH(int idConfig) throws SQLException{
		loadLH(idConfig);
		return listLH;
	}
	
	public static void main(String[] args) throws SQLException {
		LoadLopHocFromStaging load = new LoadLopHocFromStaging();
		ArrayList<LopHoc> List = load.getStagingLH(5);
		for (LopHoc lopHoc : List) {
			System.out.println(lopHoc.toString());
		}
	}
}
