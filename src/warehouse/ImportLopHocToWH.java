package warehouse;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;

public class ImportLopHocToWH {
	public void importLH(int idConfig) throws SQLException, ClassNotFoundException {
		ConnectDatabase conDB = new ConnectDatabase();
		String ms = "";
		String sql = "";
		String iDFile = null;
		Config config = new LoadConfig().getConfig(idConfig);
		Statement warehouse = null;
		String TBNameWH = null;
		ArrayList<LopHoc> listLH = new LoadLopHocFromStaging().getStagingLH(idConfig);
		LopHoc lh = null;
		// Lưu lại thời gian bắt đầu load dữ liệu
		String date = LocalDate.now().toString();
		String time = LocalTime.now().toString().substring(0, 8);
		// Kết nối các database
		
		try {
			warehouse = conDB.connectWarehouseDB(idConfig).createStatement();
		} catch (Exception e) {
			ms += "Lỗi kết nối warehouse" + ": " + e.getMessage() + " \n";
		}
		if (listLH.isEmpty()) ms += "Lỗi load staging\n";
		if (config==null) ms += "Lỗi load config\n";
		// import
		if (config != null && warehouse != null && !listLH.isEmpty()) {
			TBNameWH = config.gettBNameWH();
			int index = 0;
			while (index<listLH.size() && ms.equals("")) {
				ResultSet warehouseRec = null;
				lh = listLH.get(index);
				index++;
				String maLopHoc = lh.getMaLopHoc();
				String maMH = lh.getMaMH();
				String namHoc = lh.getNamHoc();
				iDFile = lh.getIdFile();
				// Kiểm tra xem trong warehouse có chứa lớp học nào có maLopHoc hay chưa
				sql = "Select * from " + TBNameWH + " where " + TBNameWH + ".maLopHoc = '" + maLopHoc +"' and " + TBNameWH
						+ ".dt_expired = '9999-12-31 00:00:00'";
					try {
						warehouseRec = warehouse.executeQuery(sql);
						
					} catch (Exception e) {
						ms += e.getMessage() + " \n";
					}
					// Kiểm tra xem môn học có tồn tại trong warehouse hay không
					if (warehouseRec.next()) {
						//Đã tồn tại, kiểm tra các field còn lại xem khác biệt hay không
						if (!warehouseRec.getString(3).equals(maMH)
								|| !warehouseRec.getString(4).equals(namHoc)) {

							try {
								//Set dt_expired của dữ liệu cũ thành thời gian hiện tại
								sql = "Update " + TBNameWH + " set dt_expired='2013-12-31 00:00:00.000', flag='update'"
										+ " where maLopHoc ='"+ maLopHoc + "' and dt_expired = '9999-12-31 00:00:00.000'";
								warehouse.executeLargeUpdate(sql);

							} catch (Exception e) {
								ms += "Lỗi cập nhật dt_Expired của Lớp học có mã = " + maLopHoc + ": " + e.toString()
										+ " \n";
							}

							try {
								//Insert dữ liệu mới vào warehouse
								sql = "Insert into " + TBNameWH
										+ " (maLopHoc, maMH, namHoc, idFile, dt_Expired, flag)"
										+ "values('"+ maLopHoc + "','" + maMH + "','" + namHoc+ "','" 
										+ iDFile + "','9999-12-31 00:00:00','loading')";
								warehouse.executeLargeUpdate(sql);
							} catch (Exception e) {
								ms += "Lỗi import lớp học có mã = " + maLopHoc + ": " + e.getMessage() + " \n";
							}
						}
					} else {
						try {
							//Insert dữ liệu mới vào warehouse
							sql = "Insert into " + TBNameWH
									+ " (maLopHoc, maMH, namHoc, idFile, dt_Expired, flag)"
									+ "values('"+ maLopHoc + "','" + maMH + "','" + namHoc+ "','" 
									+ iDFile + "','9999-12-31 00:00:00','loading')";
							warehouse.executeLargeUpdate(sql);
						} catch (Exception e) {
							ms += "Lỗi import lớp học có mã = " + maLopHoc + ": " + e.getMessage() + " \n";
						}
					}
			}
		}
		iDFile = lh.getIdFile();
		if (ms.equals("")) {
			ms += "File " + iDFile + " imported";
			//Cập nhật trạng thái
			sql = "Update " + TBNameWH + " set flag='finish' where flag ='loading' or flag='update'";
			warehouse.executeLargeUpdate(sql);
			//Tạo câu sql ghi log
			sql = "Insert into Log(idFile, beginTime, finishTime, states) values('" + iDFile + "','" + date + " " + time
					+ "',now(),'Finish')";
		} else {
			ms += "File " + iDFile + " Error";
			//Xóa toàn bộ dữ liệu của file mới đưa vào trong phiên làm việc bị lỗi
			sql="DELETE FROM "+ TBNameWH +" WHERE flag = 'loading'";
			warehouse.executeLargeUpdate(sql);
			//Cập nhật lại dt_Expired đã sửa đổi trong phiên làm việc bị lỗi
			sql = "Update " + TBNameWH + " set dt_expired= '9999-12-31 00:00:00.000', flag='finish' where flag='update'";
			warehouse.executeLargeUpdate(sql);
			//Tạo câu sql ghi log
			sql = "Insert into Log(idFile, beginTime, finishTime, states) values('" + iDFile + "','" + date + " " + time
					+ "',now(),'Error')";
		}
		try {
			conDB.connectLog(idConfig).createStatement().executeLargeUpdate(sql);
		} catch (Exception e) {
			ms += "Lỗi kết nối Log" + ": " + e.getMessage() + " \n";
		}
		System.out.println(ms);
		// Gửi mail thông báo
		// SendMail.send(ms);
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		ImportLopHocToWH im = new ImportLopHocToWH();
		im.importLH(5);
	}
}
