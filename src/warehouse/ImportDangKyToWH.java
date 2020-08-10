package warehouse;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;

public class ImportDangKyToWH {
	public void importDK(int idConfig) throws SQLException {
		ConnectDatabase conDB = new ConnectDatabase();
		String ms = "";
		String sql = "";
		String iDFile = null;
		Config config = new LoadConfig().getConfig(idConfig);
		Statement warehouse = null;
		String TBNameWH = null;
		ArrayList<DangKy> listDK = new LoadDangKyFromStaging().getStagingDK(idConfig);
		DangKy dk = null;
		// Lưu lại thời gian bắt đầu load dữ liệu
		String date = LocalDate.now().toString();
		String time = LocalTime.now().toString().substring(0, 8);
		// Kết nối các database

		try {
			warehouse = conDB.connectWarehouseDB(idConfig).createStatement();
		} catch (Exception e) {
			ms += "Lỗi kết nối warehouse" + ": " + e.getMessage() + " \n";
		}
		if (listDK.isEmpty())
			ms += "Lỗi load staging\n";
		if (config == null)
			ms += "Lỗi load config\n";
		// import
		if (config != null && warehouse != null && !listDK.isEmpty()) {
			TBNameWH = config.gettBNameWH();
			int index = 0;
			while (index < listDK.size() && ms.equals("")) {
				Transform changeForm = new Transform();
				ResultSet warehouseRec = null;
				dk = listDK.get(index);
				index++;
				String maDK = dk.getMaDK();
				String maSV = "";
				String maLopHoc = "";
				int tgDk = 0;
				try{
				maSV = changeForm.transformSVDim(dk.getMaSV(),idConfig);
				}
				catch (Exception e) {
					ms += "Lỗi: transform maSV của sinh viên có mã " + dk.getMaSV() + "\n";
				}
				try{
				maLopHoc = changeForm.transformMHDim(dk.getMaLopHoc(), idConfig);}
				catch (Exception e) {
					ms += "Lỗi: transform maLopHoc của lớp học có mã " + dk.getMaLopHoc() + "\n";
				}
				try{
				tgDk = changeForm.transformDayDim(dk.getThoiGianDK());
				}
				catch (Exception e) {
					ms += "Lỗi: dữ liệu ngày đăng ký không đúng định dạng \n";
				}
				iDFile = dk.getIdFile();
				int sk = 0;
				Date_Dim dateDim=new Date_Dim();
				// Kiểm tra xem trong warehouse có chứa đăng ký hay chưa
				sql = "Select * from " + TBNameWH + " where maDK = '" + maDK + "' and dt_expired = '9999-12-31 00:00:00'";
				warehouseRec = warehouse.executeQuery(sql);
				// Kiểm tra xem môn học có tồn tại trong warehouse hay không
				if (warehouseRec.next()) {
					// Đã tồn tại, kiểm tra các field còn lại xem khác biệt hay
					// không
					if (Integer.valueOf(warehouseRec.getString(5))!=tgDk) {
						try {
							// Set dt_expired của dữ liệu cũ thành thời gian
							// hiện tại
							sql = "Update " + TBNameWH + " set dt_expired='2013-12-31 00:00:00.000', flag='update'"
									+ " where maDK = '" + maDK + "' and dt_expired = '9999-12-31 00:00:00'";
							warehouse.executeLargeUpdate(sql);

						} catch (Exception e) {
							ms += "Lỗi cập nhật dt_Expired của đăng ký có mã = " + maDK + ": " + e.toString()
									+ " \n";
						}

						try {
							// Insert dữ liệu mới vào warehouse
							sql = "Insert into " + TBNameWH + " (maDK, maSV, maLopHoc, thoiGianDK, idFile, dt_Expired, flag)"
									+ "values('" + maDK + "','" + maSV + "','" + maLopHoc + "','" + String.valueOf(sk) + "','" + iDFile
									+ "','9999-12-31 00:00:00','loading')";
							warehouse.executeLargeUpdate(sql);
						} catch (Exception e) {
							ms += "Lỗi import lớp học có mã = " + maLopHoc + ": " + e.getMessage() + " \n";
						}
					}
				} else {
					try {
						// Insert dữ liệu mới vào warehouse
						sql = "Insert into " + TBNameWH + " (maDK, maSV, maLopHoc, thoiGianDK, idFile, dt_Expired, flag)"
								+ "values('" + maDK + "','" + maSV + "','" + maLopHoc + "','" + String.valueOf(sk) + "','" + iDFile
								+ "','9999-12-31 00:00:00','loading')";
						warehouse.executeLargeUpdate(sql);
					} catch (Exception e) {
						ms += "Lỗi import đăng ký có mã = " + maDK + ": " + e.getMessage() + " \n";
					}
				}
			}
		}
		if (ms.equals("")) {
			ms += "File " + iDFile + " imported";
			// Cập nhật trạng thái
			sql = "Update " + TBNameWH + " set flag='finish' where flag ='loading' or flag='update'";
			warehouse.executeLargeUpdate(sql);
			// Tạo câu sql ghi log
			sql = "Insert into Log(idFile, beginTime, finishTime, states) values('" + iDFile + "','" + date + " " + time
					+ "',now(),'Finish')";
		} else {
			ms += "File " + iDFile + " Error";
			// Xóa toàn bộ dữ liệu của file mới đưa vào trong phiên làm việc bị
			// lỗi
			sql = "DELETE FROM " + TBNameWH + " WHERE flag = 'loading'";
			warehouse.executeLargeUpdate(sql);
			// Cập nhật lại dt_Expired đã sửa đổi trong phiên làm việc bị lỗi
			sql = "Update " + TBNameWH
					+ " set dt_expired= '9999-12-31 00:00:00.000', flag='finish' where flag='update'";
			warehouse.executeLargeUpdate(sql);
			// Tạo câu sql ghi log
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
		ImportDangKyToWH im = new ImportDangKyToWH();
		im.importDK(6);
	}
}
