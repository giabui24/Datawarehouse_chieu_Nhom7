package warehouse;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalTime;

public class Warehouse {
	public void importSV() throws SQLException, ClassNotFoundException {
		ConnectDatabase conDB = new ConnectDatabase();
		String ms;
		String sql = "";
		String IDFile = null;
		Statement sta = null;
		// Lưu lại thời gian bắt đầu load dữ liệu
		String date = LocalDate.now().toString();
		String time = LocalTime.now().toString().substring(0, 8);

		ResultSet staging = conDB.loadSDB();
		ResultSet config = conDB.loadDBConfig();
		Statement warehouse = conDB.connectDDB().createStatement();
		if (staging != null && config != null && warehouse != null) {
			config.next();
			String desDB = config.getString(9);
			int numberCol = 12;
			while (staging.next()) {
				String maSV = staging.getString(2);
				// Kiểm tra xem trong warehouse có chứa sinh viên nào có mã
				// trùng với mã maSV sắp insert vào hay không
				sql = "Select * from " + desDB + " where " + desDB + ".maSV = " + maSV + " and " + desDB
						+ ".dt_expired = '9999-12-31 00:00:00'";
				ResultSet warehouseRec = warehouse.executeQuery(sql);
				if (warehouseRec.next()) {
					// Trùng maSV nhưng khác một số field
					boolean check = false;
					for (int i = 4; i <= numberCol; i++)
						if (!warehouseRec.getString(i).equals(staging.getString(i - 1)))
							check = true;
					if (check) {
						// Set DT_expired của dữ liệu cũ thành thời gian hiện tại
						sql = "Update " + desDB + " set dt_expired=now() where maSV =" + maSV
								+ " and dt_expired = '9999-12-31 00:00:00'";
						warehouse.executeLargeUpdate(sql);
						// insert dữ liệu mới vào warehouse
						sql = "Insert into " + desDB
								+ " (STT, maSV, ho, ten, ngaySinh, maLop, tenLop, dt, queQuan, email, idFile, dt_Expired) values("
								+ staging.getString(1) + ",'" + staging.getString(2) + "','" + staging.getString(3)
								+ "','" + staging.getString(4) + "','" + staging.getString(5) + "','"
								+ staging.getString(6) + "','" + staging.getString(7) + "','" + staging.getString(8)
								+ "','" + staging.getString(9) + "','" + staging.getString(10) + "','"
								+ staging.getString(11) + "','9999-12-31 00:00:00')";
						warehouse.executeLargeUpdate(sql);
					}
				} else {
					// insert dữ liệu mới vào warehouse
					sql = "Insert into " + desDB
							+ " (STT, maSV, ho, ten, ngaySinh, maLop, tenLop, dt, queQuan, email, idFile, dt_Expired) values("
							+ staging.getString(1) + ",'" + staging.getString(2) + "','" + staging.getString(3) + "','"
							+ staging.getString(4) + "','" + staging.getString(5) + "','" + staging.getString(6) + "','"
							+ staging.getString(7) + "','" + staging.getString(8) + "','" + staging.getString(9) + "','"
							+ staging.getString(10) + "','" + staging.getString(11) + "','9999-12-31 00:00:00.000')";
					warehouse.executeLargeUpdate(sql);
				}
				IDFile = staging.getString(11);
			}
			// Ghi log

			sql = "Insert into Log(idFile, beginTime, finishTime, states) values('" + IDFile + "','" + date + " " + time
					+ "',now(),'Finish')";
			ms = "File " + IDFile + " imported";
		} else {
			// TODO Auto-generated catch block
			sql = "Insert into Log(idFile, beginTime, finishTime, states) values('" + IDFile + "','" + date + " " + time
					+ "',now(),'Error')";
			ms = "Error";
			System.out.println("Kết nối thất bại");
		}
		Connection conn = conDB.connectLog();
		sta = conn.createStatement();
		sta.executeLargeUpdate(sql);

		// Gửi mail thông báo
		SendMail sm = new SendMail();
		sm.send(ms);
	}

	/*public void importMH() throws SQLException, ClassNotFoundException {
		ConnectDatabase conDB = new ConnectDatabase();
		String ms;
		String sql = "";
		String IDFile = null;
		Statement sta = null;
		// Lưu lại thời gian bắt đầu load dữ liệu
		String date = LocalDate.now().toString();
		String time = LocalTime.now().toString().substring(0, 8);

		ResultSet staging = conDB.loadSDB();
		ResultSet config = conDB.loadDBConfig();
		Statement warehouse = conDB.connectDDB().createStatement();
		if (staging != null && config != null && warehouse != null) {
			config.next();
			String desDB = config.getString(9);
			int numberCol = 7;
			while (staging.next()) {
				String stt = staging.getString(1);
				// Kiểm tra xem trong warehouse có chứa môn học nào có số thứ tự
				// trùng với môn học có số thứ tự sắp insert vào hay không
				sql = "Select * from " + desDB + " where " + desDB + ".stt = " + stt + " and " + desDB
						+ ".dt_expired = '9999-12-31 00:00:00'";
				ResultSet warehouseRec = warehouse.executeQuery(sql);
				if (warehouseRec.next()) {
					// Trùng maSV nhưng khác một số field
					boolean check = false;
					for (int i = 3; i <= numberCol; i++)
						if (!warehouseRec.getString(i).equals(staging.getString(i - 1)))
							check = true;
					if (check) {
						// Set DT_expired của dữ liệu cũ thành thời gian hiện
						// tại
						sql = "Update " + desDB + " set dt_expired='2013-12-31 00:00:00' where stt =" + stt
								+ " and dt_expired = '9999-12-31 00:00:00'";
						warehouse.executeLargeUpdate(sql);
						// insert dữ liệu mới vào warehouse
						sql = "Insert into " + desDB
								+ " (stt, maMH, tenMH, tc, khoaBMQuanLy, khoaBMSuDung,dt_Expired) values("
								+ staging.getString(1) + ",'" + staging.getString(2) + "','" + staging.getString(3)
								+ "'," + staging.getString(4) + ",'" + staging.getString(5) + "','"
								+ staging.getString(6) + "','9999-12-31 00:00:00')";
						warehouse.executeLargeUpdate(sql);
					}
				} else {
					// insert dữ liệu mới vào warehouse
					sql = "Insert into " + desDB
							+ " (stt, maMH, tenMH, tc, khoaBMQuanLy, khoaBMSuDung,dt_Expired) values("
							+ staging.getString(1) + ",'" + staging.getString(2) + "','" + staging.getString(3) + "',"
							+ staging.getString(4) + ",'" + staging.getString(5) + "','" + staging.getString(6)
							+ "','9999-12-31 00:00:00')";
					warehouse.executeLargeUpdate(sql);
				}
				IDFile = staging.getString(7);
			}
			// Ghi log

			sql = "Insert into Log(idFile, beginTime, finishTime, states) values('" + IDFile + "','" + date + " " + time
					+ "',now(),'Finish')";
			ms = "File " + IDFile + " imported";
		} else {
			// TODO Auto-generated catch block
			sql = "Insert into Log(idFile, beginTime, finishTime, states) values('" + IDFile + "','" + date + " " + time
					+ "',now(),'Error')";
			ms = "Error";
			System.out.println("Kết nối thất bại");
		}
		Connection conn = conDB.connectLog();
		sta = conn.createStatement();
		sta.executeLargeUpdate(sql);

		// Gửi mail thông báo
		SendMail sm = new SendMail();
		// sm.send(ms);
	}*/

	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		Warehouse ware = new Warehouse();
	}
}
