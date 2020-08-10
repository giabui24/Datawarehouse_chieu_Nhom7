package warehouse;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class LoadMonHocFromStaging {
	ArrayList<MonHoc> listMH = new ArrayList<>();
	
	
	public void loadMH(int idConfig) throws SQLException {
		ConnectDatabase conDB = new ConnectDatabase();
		ResultSet staging = null;
		MonHoc mh;
		boolean check = true;
		try {
			staging = conDB.loadStagingDB(idConfig);
		}
		catch (Exception e) {
			// TODO: handle exception
		}
		if (staging != null) {
			while (staging.next() && check) {
				mh = new MonHoc(staging.getString(1), staging.getString(2), staging.getString(3), staging.getString(4), staging.getString(5), staging.getString(6), staging.getString(7));
				listMH.add(mh);
			}
		}
	}
	public ArrayList<MonHoc> getStagingMH(int idConfig) throws SQLException{
		loadMH(idConfig);
		return listMH;
	}
	
	public static void main(String[] args) throws SQLException {
		LoadMonHocFromStaging load=new LoadMonHocFromStaging();
		ArrayList<MonHoc> list= load.getStagingMH(3);
		for (MonHoc monHoc : list) {
			System.out.println(monHoc.toString());
			
		}
	}
}
