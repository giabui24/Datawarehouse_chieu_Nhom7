package warehouse;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;

public class ImportSinhVienToWH {
	public void importSV(int idConfig) throws SQLException {
		ConnectDatabase conDB = new ConnectDatabase();
		int sk = 0;
		String ms = "";
		String sql = "";
		String IDFile = null;
		Statement warehouse = null;
		String TBNameWH = null;
		SinhVien sv = null;
		ArrayList<SinhVien> listSV = new LoadSinhVienFromStaging().getStagingSV(idConfig);
		Config config = new LoadConfig().getConfig(idConfig);
		// Lưu lại thời gian bắt đầu load dữ liệu
		String date = LocalDate.now().toString();
		String time = LocalTime.now().toString().substring(0, 8);
		// Kết nối database warehouse
		try {
			warehouse = conDB.connectWarehouseDB(idConfig).createStatement();
		} catch (Exception e) {
			ms += "Lỗi kết nối warehouse\n";
		}
		if (listSV.isEmpty()) ms += "Lỗi load staging\n";
		if (config==null) ms += "Lỗi load config\n";
		// import
		if (config != null && warehouse != null && !listSV.isEmpty()) {
			TBNameWH = config.gettBNameWH();
			int index = 0;
			ResultSet warehouseRec = null;
			while (index<listSV.size() && ms.equals("")) {
				Date_Dim dateDim = new Date_Dim();
				sv = listSV.get(index);
				index++;
				//Lấy thông tin SV
				String maSV = sv.getMaSV();
				String ho = sv.getHo();
				String ten = sv.getTen();
				String ngaySinh = sv.getNgaySinh();
				String maLop = sv.getMaLop();
				String tenLop = sv.getTenLop();
				String dt = sv.getDt();
				String email = sv.getEmail();
				String queQuan = sv.getQueQuan();
				String idFile = sv.getIdFile();
				try {
					int y = Integer.valueOf(ngaySinh.substring(6, 10));
					int m = Integer.valueOf(ngaySinh.substring(3, 5));
					int d = Integer.valueOf(ngaySinh.substring(0, 2));
//					System.out.println(y);
//					System.out.println(m);
//					System.out.println(d);
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
						if (!warehouseRec.getString(3).equals(ho)
								|| !warehouseRec.getString(4).equals(ten)
								|| sk != Integer.valueOf(warehouseRec.getString(5))
								|| !warehouseRec.getString(6).equals(maLop)
								|| !warehouseRec.getString(7).equals(tenLop)
								|| !warehouseRec.getString(8).equals(dt)
								|| !warehouseRec.getString(9).equals(email)
								|| !warehouseRec.getString(10).equals(queQuan)) {

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
									+ " (maSV, ho, ten, ngaySinh, maLop, tenLop, dt, email, queQuan, idFile, dt_Expired, flag)"
									+ "values('"+ maSV + "','" + ho + "','" + ten
									+ "','" + String.valueOf(sk) + "','" + maLop + "','"
									+ tenLop + "'," + dt + ",'" + email
									+ "','" + queQuan + "','" + idFile
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
									+ "values('"+ maSV + "','" + ho + "','" + ten
									+ "','" + String.valueOf(sk) + "','" + maLop + "','"
									+ tenLop + "'," + dt + ",'" + email
									+ "','" + queQuan + "','" + idFile
									+ "','9999-12-31 00:00:00.000','loading')";
							warehouse.executeLargeUpdate(sql);
						} catch (Exception e) {
							ms += "Lỗi import của sinh viên có mã " + maSV + ": " + e.getMessage() + " \n";
						}
					}
					IDFile = sv.getIdFile();
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
//			sql="DELETE FROM "+ TBNameWH +" WHERE flag = 'loading'";
//			warehouse.executeLargeUpdate(sql);
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
	
	public static void main(String[] args) throws SQLException {
		ImportSinhVienToWH load=new ImportSinhVienToWH();
		load.importSV(1);
	}
}
