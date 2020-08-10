package warehouse;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringTokenizer;

public class Transform {
	public int transformDayDim(String dateTime) throws SQLException, ClassNotFoundException {
		Date_Dim dim =new Date_Dim();
		StringTokenizer ns = new StringTokenizer(dateTime,"/");
		int d = Integer.valueOf(ns.nextToken());
		int m = Integer.valueOf(ns.nextToken());
		int y= Integer.valueOf(ns.nextToken());
		String dt=y+"/"+m+"/"+d;
		int sk;
		sk = dim.getSKDateDim(dt);
		if (sk==0) dim.insertDateDim(dt);
		sk = dim.getSKDateDim(dt);
		return sk;
	}
	
	public String transformSVDim(String maSV, int idConfig) throws ClassNotFoundException, SQLException{
		ConnectDatabase conDB = new ConnectDatabase();
		Connection con=conDB.connectWarehouseDB(idConfig);
		String sql = "Select * from sinhvien where maSV='"+maSV+"' and dt_Expired='9999-12-31 00:00:00.000'";
		ResultSet re = con.createStatement().executeQuery(sql);
		re.next();
		return re.getString("id");
	}
	
	public String transformMHDim(String maMH, int idConfig) throws ClassNotFoundException, SQLException{
		ConnectDatabase conDB = new ConnectDatabase();
		Connection con=conDB.connectWarehouseDB(idConfig);
		String sql = "Select * from monhoc where maMH='"+maMH+"' and dt_Expired='9999-12-31 00:00:00.000'";
		ResultSet re = con.createStatement().executeQuery(sql);
		re.next();
		return re.getString("id");
	}
	
	public String transformLHDim(String maLH, int idConfig) throws ClassNotFoundException, SQLException{
		ConnectDatabase conDB = new ConnectDatabase();
		Connection con=conDB.connectWarehouseDB(idConfig);
		String sql = "Select * from lophoc where maLopHoc='"+maLH+"' and dt_Expired='9999-12-31 00:00:00.000'";
		ResultSet re = con.createStatement().executeQuery(sql);
		re.next();
		return re.getString("id");
	}

	
	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		Transform changeForm=new Transform();
		System.out.println(changeForm.transformSVDim("14153040", 1));
	}
}
