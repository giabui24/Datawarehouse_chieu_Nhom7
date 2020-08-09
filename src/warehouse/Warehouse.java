import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalTime;

public class Warehouse {
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Warehouse ware = new Warehouse();
		ware.importMH(4);;
	}

	public void importSV(int idConfig) throws SQLException {
		ConnectDatabase conDB = new ConnectDatabase();
		int sk = 0;
		String ms = "";
		String sql = "";
		String IDFile = null;
		ResultSet staging = null;
		ResultSet config = null;
		Statement warehouse = null;
		String TBNameWH = null;
		// Lưu lại thời gian bắt đầu load dữ liệu
		String date = LocalDate.now().toString();
		String time = LocalTime.now().toString().substring(0, 8);
		// Kết nối các database
		try {
			config = conDB.loadDBConfig(idConfig);

		} catch (Exception e) {
			ms += "Lỗi kết nối config" + ": " + e.getMessage() + " \n";
		}
		try {
			staging = conDB.loadStagingDB(idConfig);
		} catch (Exception e) {
			ms += "Lỗi kết nối staging" + ": " + e.getMessage() + " \n";
		}
		try {
			warehouse = conDB.connectWarehouseDB(idConfig).createStatement();
		} catch (Exception e) {
			ms += "Lỗi kết nối warehouse" + ": " + e.getMessage() + " \n";
		}
		// import
		if (staging != null && config != null && warehouse != null) {
			config.next();
			TBNameWH = config.getString("TBNameWH");
			while (staging.next() && ms.equals("")) {
				Date_Dim dateDim = new Date_Dim();
				ResultSet warehouseRec = null;
				String maSV = staging.getString(2);
				try {
					int y = Integer.valueOf(staging.getString(5).substring(6, 10));
					int m = Integer.valueOf(staging.getString(5).substring(3, 5));
					int d = Integer.valueOf(staging.getString(5).substring(0, 2));
					// Lấy skDateDim trong bảng dataDim
					sk = dateDim.getSKDateDim(String.valueOf(y) + "-" + String.valueOf(m) + "-" + String.valueOf(d));
					// Lấy thông tin sinh viên có maSV trong warehouse
					sql = "Select * from " + TBNameWH + " where " + TBNameWH + ".maSV = " + maSV + " and " + TBNameWH
							+ ".dt_Expired = '9999-12-31 00:00:00.000'";
					try {
						warehouseRec = warehouse.executeQuery(sql);
					} catch (Exception e) {
						ms += "Lỗi kiểm tra maSV của sinh viên có mã " + maSV + ": " + e.getMessage() + " \n";
					}
					// Kiểm tra xem sinh viên có maSV có tồn tại trong warehouse hay không
					if (warehouseRec.next()) {
						// Tồn tại sinh viên có maSV, kiểm tra các field còn lại xem khác biệt hay không
						if (!warehouseRec.getString(3).equals(staging.getString(3))
								|| !warehouseRec.getString(4).equals(staging.getString(4))
								|| sk != Integer.valueOf(warehouseRec.getString(5))
								|| !warehouseRec.getString(6).equals(staging.getString(6))
								|| !warehouseRec.getString(7).equals(staging.getString(7))
								|| !warehouseRec.getString(8).equals(staging.getString(8))
								|| !warehouseRec.getString(9).equals(staging.getString(9))
								|| !warehouseRec.getString(10).equals(staging.getString(10))) {

							try {
								// Set DT_expired của dữ liệu cũ thành thời gian hiện tại
								sql = "Update " + TBNameWH + " set dt_expired=now(), flag='update' where maSV ="
										+ maSV + " and dt_expired = '9999-12-31 00:00:00.000'";
								warehouse.executeLargeUpdate(sql);

							} catch (Exception e) {
								ms += "Lỗi cập nhật dt_Expired của sinh viên có mã " + maSV + ": " + e.getMessage()
										+ " \n";
							}

							// insert dữ liệu mới vào warehouse
							sql = "Insert into " + TBNameWH
									+ " (maSV, ho, ten, ngaySinh, maLop, tenLop, dt, email, queQuan, idFile, dt_Expired, flag) values('"
									+ staging.getString(2) + "','" + staging.getString(3) + "','" + staging.getString(4)
									+ "','" + String.valueOf(sk) + "','" + staging.getString(6) + "','"
									+ staging.getString(7) + "'," + staging.getString(8) + ",'" + staging.getString(9)
									+ "','" + staging.getString(10) + "','" + staging.getString(11)
									+ "','9999-12-31 00:00:00.000','loading')";
							try {
								warehouse.executeLargeUpdate(sql);
							} catch (Exception e) {
								ms += "Lỗi import của sinh viên có mã " + maSV + ": " + e.getMessage() + " \n";
							}
						}
					} else {
						try {
							// insert dữ liệu mới vào warehouse
							sql = "Insert into " + TBNameWH
									+ " (maSV, ho, ten, ngaySinh, maLop, tenLop, dt, email, queQuan, idFile, dt_Expired, flag) values('"
									+ staging.getString(2) + "','" + staging.getString(3) + "','" + staging.getString(4)
									+ "','" + String.valueOf(sk) + "','" + staging.getString(6) + "','"
									+ staging.getString(7) + "'," + staging.getString(8) + ",'" + staging.getString(9)
									+ "','" + staging.getString(10) + "','" + staging.getString(11)
									+ "','9999-12-31 00:00:00.000', 'loading')";
							warehouse.executeLargeUpdate(sql);
						} catch (Exception e) {
							ms += "Lỗi import của sinh viên có mã " + maSV + ": " + e.getMessage() + " \n";
						}
					}
					IDFile = staging.getString(11);
				} catch (Exception e) {
					ms += "Lỗi dữ liệu ngày sinh của sinh viên có mã " + maSV + " không đúng định dạng \n";
				}
			}
		}
		if (ms.equals("")) {
			ms += "File " + IDFile + " imported";
			//Cập nhật trạng thái
			sql = "Update " + TBNameWH + " set flag='finish' where flag ='loading' or flag='update'";
			warehouse.executeLargeUpdate(sql);
			//Tạo câu sql ghi log
			sql = "Insert into Log(idFile, beginTime, finishTime, states) values('" + IDFile + "','" + date + " " + time
					+ "',now(),'Finish')";
		} else {
			ms += "File " + IDFile + " Error";
			//Xóa toàn bộ dữ liệu của file mới đưa vào trong phiên làm việc bị lỗi
			sql="DELETE FROM "+ TBNameWH +" WHERE flag = 'loading'";
			warehouse.executeLargeUpdate(sql);
			//Cập nhật lại dt_Expired đã sửa đổi trong phiên làm việc bị lỗi
			sql = "Update " + TBNameWH + " set dt_expired= '9999-12-31 00:00:00.000', flag='finish' where flag='update'";
			warehouse.executeLargeUpdate(sql);
			//Tạo câu sql ghi log
			sql = "Insert into Log(idFile, beginTime, finishTime, states) values('" + IDFile + "','" + date + " " + time
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

	public void importMH(int idConfig) throws SQLException, ClassNotFoundException {
		ConnectDatabase conDB = new ConnectDatabase();
		String ms = "";
		String sql = "";
		String IDFile = null;
		ResultSet staging = null;
		ResultSet config = null;
		Statement warehouse = null;
		String TBNameWH = null;
		// Lưu lại thời gian bắt đầu load dữ liệu
		String date = LocalDate.now().toString();
		String time = LocalTime.now().toString().substring(0, 8);
		// Kết nối các database
		try {
			config = conDB.loadDBConfig(idConfig);

		} catch (Exception e) {
			ms += "Lỗi kết nối config" + ": " + e.getMessage() + " \n";
		}
		try {
			staging = conDB.loadStagingDB(idConfig);
		} catch (Exception e) {
			ms += "Lỗi kết nối staging" + ": " + e.getMessage() + " \n";
		}
		try {
			warehouse = conDB.connectWarehouseDB(idConfig).createStatement();
		} catch (Exception e) {
			ms += "Lỗi kết nối warehouse" + ": " + e.getMessage() + " \n";
		}
		// import
		if (staging != null && config != null && warehouse != null) {
			config.next();
			TBNameWH = config.getString("TBNameWH");
			while (staging.next() && ms.equals("")) {
				ResultSet warehouseRec = null;
				String stt = staging.getString(1);
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
						if (!warehouseRec.getString(3).equals(staging.getString(2))
								|| !warehouseRec.getString(4).equals(staging.getString(3))
								|| !warehouseRec.getString(5).equals(staging.getString(4))
								|| !warehouseRec.getString(6).equals(staging.getString(5))
								|| !warehouseRec.getString(7).equals(staging.getString(6))) {

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
										+ " (stt, maMH, tenMH, tc, khoaBMQuanLy, khoaBMSuDung, idFile, dt_Expired, flag) values("
										+ staging.getString(1) + ",'" + staging.getString(2) + "','" + staging.getString(3)
										+ "'," + staging.getString(4) + ",'" + staging.getString(5) + "','"
										+ staging.getString(6) + "','" + staging.getString(7) + "','9999-12-31 00:00:00','loading')";
								warehouse.executeLargeUpdate(sql);
							} catch (Exception e) {
								ms += "Lỗi import môn học có số thứ tự= " + stt + ": " + e.getMessage() + " \n";
							}
						}
					} else {
						try {
							//Insert dữ liệu mới vào warehouse
							sql = "Insert into " + TBNameWH
									+ " (stt, maMH, tenMH, tc, khoaBMQuanLy, khoaBMSuDung, idFile, dt_Expired, flag) values("
									+ staging.getString(1) + ",'" + staging.getString(2) + "','" + staging.getString(3)
									+ "'," + staging.getString(4) + ",'" + staging.getString(5) + "','"
									+ staging.getString(6) + "','" + staging.getString(7) + "','9999-12-31 00:00:00','loading')";
							warehouse.executeLargeUpdate(sql);
						} catch (Exception e) {
							ms += "Lỗi import môn học có số thứ tự= " + stt + ": " + e.getMessage() + " \n";
						}
					}
					IDFile = staging.getString(7);
			}
		}
		if (ms.equals("")) {
			ms += "File " + IDFile + " imported";
			//Cập nhật trạng thái
			sql = "Update " + TBNameWH + " set flag='finish' where flag ='loading' or flag='update'";
			warehouse.executeLargeUpdate(sql);
			//Tạo câu sql ghi log
			sql = "Insert into Log(idFile, beginTime, finishTime, states) values('" + IDFile + "','" + date + " " + time
					+ "',now(),'Finish')";
		} else {
			ms += "File " + IDFile + " Error";
			//Xóa toàn bộ dữ liệu của file mới đưa vào trong phiên làm việc bị lỗi
			sql="DELETE FROM "+ TBNameWH +" WHERE flag = 'loading'";
			warehouse.executeLargeUpdate(sql);
			//Cập nhật lại dt_Expired đã sửa đổi trong phiên làm việc bị lỗi
			sql = "Update " + TBNameWH + " set dt_expired= '9999-12-31 00:00:00.000', flag='finish' where flag='update'";
			warehouse.executeLargeUpdate(sql);
			//Tạo câu sql ghi log
			sql = "Insert into Log(idFile, beginTime, finishTime, states) values('" + IDFile + "','" + date + " " + time
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
}
