package warehouse;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.joda.time.DateTime;

public class ImportSinhVienToWH {
	public void importSV(int idConfig) throws SQLException {
		ConnectDatabase conDB = new ConnectDatabase();
		String ms = "";
		String sql = "";
		String IDFile = null;
		Statement warehouse = null;
		String TBNameWH = null;
		SinhVien sv = null;
		Config config = new LoadConfig().getConfig(idConfig);
		ArrayList<SinhVien> listSV = new LoadSinhVienFromStaging().getStagingSV(idConfig);
		// Lưu lại thời gian bắt đầu load dữ liệu
		String date = LocalDate.now().toString();
		String time = LocalTime.now().toString().substring(0, 8);
		// Kết nối database warehouse
		try {
			warehouse = conDB.connectWarehouseDB(idConfig).createStatement();
		} catch (Exception e) {
			ms += "Lỗi kết nối warehouse\n";
		}
		if (listSV.isEmpty())
			ms += "Lỗi load staging\n";
		if (config == null)
			ms += "Lỗi load config\n";
		// import
		if (config != null && warehouse != null && !listSV.isEmpty()) {
			TBNameWH = config.gettBNameWH();
			int index = 0;
			ResultSet warehouseRec = null;
			while (index < listSV.size() && ms.equals("")) {
				Transform changeForm = new Transform();
				sv = listSV.get(index);
				index++;
				// Lấy thông tin SV
				String maSV = sv.getMaSV();
				String ho = sv.getHo();
				String ten = sv.getTen();
				int ngaySinh = 0;
				try{
				ngaySinh = changeForm.transformDayDim(sv.getNgaySinh());
				}
				catch (Exception e) {
					ms += "Lỗi: dữ liệu ngày sinh của sinh viên có mã " + maSV + " không đúng định dạng \n";
				}
				String maLop = sv.getMaLop();
				String tenLop = sv.getTenLop();
				String dt = sv.getDt();
				String email = sv.getEmail();
				String queQuan = sv.getQueQuan();
				String idFile = sv.getIdFile();
				try {
					// Lấy thông tin sinh viên có maSV trong warehouse
					sql = "Select * from " + TBNameWH + " where " + TBNameWH + ".maSV = " + maSV + " and " + TBNameWH
							+ ".dt_Expired = '9999-12-31 00:00:00.000'";
					warehouseRec = warehouse.executeQuery(sql);
				} catch (Exception e) {
					ms += "Lỗi kiểm tra maSV của sinh viên có mã " + maSV + ": " + e.getMessage() + " \n";
				}
				// Kiểm tra xem sinh viên có maSV có tồn tại trong warehouse hay
				// không
				if (warehouseRec.next()) {
					// Tồn tại sinh viên có maSV, kiểm tra các field còn lại xem
					// khác biệt hay không
					if (!warehouseRec.getString(3).equals(ho) || !warehouseRec.getString(4).equals(ten)
							|| ngaySinh != Integer.valueOf(warehouseRec.getString(5))
							|| !warehouseRec.getString(6).equals(maLop) || !warehouseRec.getString(7).equals(tenLop)
							|| !warehouseRec.getString(8).equals(dt) || !warehouseRec.getString(9).equals(email)
							|| !warehouseRec.getString(10).equals(queQuan)) {

						try {
							// Set DT_expired của dữ liệu cũ thành thời gian
							// hiện tại
							sql = "Update " + TBNameWH + " set dt_expired=now(), flag='update' where maSV =" + maSV
									+ " and dt_expired = '9999-12-31 00:00:00.000'";
							warehouse.executeLargeUpdate(sql);

						} catch (Exception e) {
							ms += "Lỗi cập nhật dt_Expired của sinh viên có mã " + maSV + ": " + e.getMessage() + " \n";
						}

						// insert dữ liệu mới vào warehouse
						sql = "Insert into " + TBNameWH
								+ " (maSV, ho, ten, ngaySinh, maLop, tenLop, dt, email, queQuan, idFile, dt_Expired, flag)"
								+ "values('" + maSV + "','" + ho + "','" + ten + "','" + String.valueOf(ngaySinh) + "','"
								+ maLop + "','" + tenLop + "'," + dt + ",'" + email + "','" + queQuan + "','" + idFile
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
								+ " (maSV, ho, ten, ngaySinh, maLop, tenLop, dt, email, queQuan, idFile, dt_Expired, flag)"
								+ "values('" + maSV + "','" + ho + "','" + ten + "','" + String.valueOf(ngaySinh) + "','"
								+ maLop + "','" + tenLop + "'," + dt + ",'" + email + "','" + queQuan + "','" + idFile
								+ "','9999-12-31 00:00:00.000','loading')";
						warehouse.executeLargeUpdate(sql);
					} catch (Exception e) {
						ms += "Lỗi import của sinh viên có mã " + maSV + ": " + e.getMessage() + " \n";
					}
				}
				IDFile = sv.getIdFile();
			}
		}
		if (ms.equals("")) {
			ms += "File " + IDFile + " imported";
			// Cập nhật trạng thái
			sql = "Update " + TBNameWH + " set flag='finish' where flag ='loading' or flag='update'";
			warehouse.executeLargeUpdate(sql);
			// Tạo câu sql ghi log
			sql = "Insert into Log(idFile, beginTime, finishTime, states) values('" + IDFile + "','" + date + " " + time
					+ "',now(),'Finish')";
		} else {
			ms += "File " + IDFile + " Error";
			// Xóa toàn bộ dữ liệu của file mới đưa vào trong phiên làm việc bị
			// lỗi
			sql = "DELETE FROM " + TBNameWH + " WHERE flag = 'loading'";
			warehouse.executeLargeUpdate(sql);
			// Cập nhật lại dt_Expired đã sửa đổi trong phiên làm việc bị lỗi
			sql = "Update " + TBNameWH
					+ " set dt_expired= '9999-12-31 00:00:00.000', flag='finish' where flag='update'";
			warehouse.executeLargeUpdate(sql);
			// Tạo câu sql ghi log
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

	public static void main(String[] args) throws SQLException {
		ImportSinhVienToWH load = new ImportSinhVienToWH();
		load.importSV(1);
	}
}
