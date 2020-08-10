package warehouse;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;

public class ImportMonHocToWH {
	public void importMH(int idConfig) throws SQLException, ClassNotFoundException {
		ConnectDatabase conDB = new ConnectDatabase();
		String ms = "";
		String sql = "";
		String iDFile = null;
		Config config = new LoadConfig().getConfig(idConfig);
		Statement warehouse = null;
		String TBNameWH = null;
		ArrayList<MonHoc> listMH = new LoadMonHocFromStaging().getStagingMH(idConfig);
		MonHoc mh = null;
		// Lưu lại thời gian bắt đầu load dữ liệu
		String date = LocalDate.now().toString();
		String time = LocalTime.now().toString().substring(0, 8);
		// Kết nối các database
		
		try {
			warehouse = conDB.connectWarehouseDB(idConfig).createStatement();
		} catch (Exception e) {
			ms += "Lỗi kết nối warehouse" + ": " + e.getMessage() + " \n";
		}
		if (listMH.isEmpty()) ms += "Lỗi load staging\n";
		if (config==null) ms += "Lỗi load config\n";
		
		// import--------------------------------------------------------------------------------
		if (config != null && warehouse != null && !listMH.isEmpty()) {
			TBNameWH = config.gettBNameWH();
			int index = 0;
			while (index<listMH.size() && ms.equals("")) {
				ResultSet warehouseRec = null;
				mh = listMH.get(index);
				index++;
				String stt = mh.getStt();
				String maMH = mh.getMaMH();
				String tenMH = mh.getTenMH();
				String tc = mh.getTc();
				String khoaBMQuanLy = mh.getKhoaBMQuanLy();
				String khoaBMSuDung = mh.getKhoaBMSuDung();
				iDFile = mh.getIdFile();
				// Kiểm tra xem trong warehouse có chứa môn học nào có số thứ tự
				// trùng với môn học có số thứ tự sắp insert vào hay không
				sql = "Select * from " + TBNameWH + " where " + TBNameWH + ".stt = " + stt + " and " + TBNameWH
						+ ".dt_expired = '9999-12-31 00:00:00'";
					try {
						warehouseRec = warehouse.executeQuery(sql);
						
					} catch (Exception e) {
						ms += "Lỗi kiểm tra môn học có số thứ tự " + stt + ": " + e.getMessage() + " \n";
					}
					// Kiểm tra xem môn học có tồn tại trong warehouse hay không
				if (warehouseRec.next()) {
						//Đã tồn tại, kiểm tra các field còn lại xem khác biệt hay không
						if (!warehouseRec.getString(3).equals(maMH)
								|| !warehouseRec.getString(4).equals(tenMH)
								|| !warehouseRec.getString(5).equals(tc)
								|| !warehouseRec.getString(6).equals(khoaBMQuanLy)
								|| !warehouseRec.getString(7).equals(khoaBMSuDung)) {

							try {
								//Set dt_expired của dữ liệu cũ thành thời gian hiện tại
								sql = "Update " + TBNameWH + " set dt_expired='2013-12-31 00:00:00.000', flag='update' where stt ="
										+ stt + " and dt_expired = '9999-12-31 00:00:00.000'";
								warehouse.executeLargeUpdate(sql);

							} catch (Exception e) {
								ms += "Lỗi cập nhật dt_Expired của môn học có số thứ tự= " + stt + ": " + e.getMessage()
										+ " \n";
							}

							try {
								//Insert dữ liệu mới vào warehouse
								sql = "Insert into " + TBNameWH
										+ " (stt, maMH, tenMH, tc, khoaBMQuanLy, khoaBMSuDung, idFile, dt_Expired, flag)"
										+ "values("+ stt + ",'" + maMH + "','" + tenMH + "'," + tc + ",'" + khoaBMQuanLy + "','"
										+ khoaBMSuDung + "','" + iDFile + "','9999-12-31 00:00:00','loading')";
								warehouse.executeLargeUpdate(sql);
							} catch (Exception e) {
								ms += "Lỗi import môn học có số thứ tự= " + stt + ": " + e.getMessage() + " \n";
							}
						}
					} else {
						try {
							//Insert dữ liệu mới vào warehouse
							sql = "Insert into " + TBNameWH
									+ " (stt, maMH, tenMH, tc, khoaBMQuanLy, khoaBMSuDung, idFile, dt_Expired, flag)"
									+ "values("+ stt + ",'" + maMH + "','" + tenMH + "'," + tc + ",'" + khoaBMQuanLy + "','"
									+ khoaBMSuDung + "','" + iDFile + "','9999-12-31 00:00:00','loading')";
							warehouse.executeLargeUpdate(sql);
						} catch (Exception e) {
							ms += "Lỗi import môn học có số thứ tự= " + stt + ": " + e.getMessage() + " \n";
						}
					}
			}
		}
		//----------------------------------------------------------------------------------------------------
		iDFile = mh.getIdFile();
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
	ImportMonHocToWH im=new ImportMonHocToWH();
	im.importMH(4);
}
}
