package etl;



import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import connection.GetConnection;
import tool.SendMailTLS;
//import warehouse.SendMail;
//xu li data tu LOCAL vao STAGING
public class Staging {
	String emailSendTo, subject,textMail;
	
	
	public void updateBulk() {
		SendMailTLS sendMail = new SendMailTLS();
		
		
		String textSendMail ="";
		Connection conn = null;
		PreparedStatement pre_control = null;
		try {
			// 1. Kết nối tới Control_DB
			conn = new GetConnection().getConnection("databasecontrol?serverTimezone=UTC");
			// 2. Tìm các file có trạng thái OK download 
			pre_control = conn.prepareStatement(
					"SELECT data_file_logs.id ,data_file_logs.your_filename,  "
							+ " data_file_logs.delimiter,data_file_logs.table_staging_load, data_file_logs.download_to_dir_local,encode,"
							+" data_file_logs.number_column from data_file_logs "
							
							+ " where "
							+ "data_file_logs.status_file like 'OK Download'  ");
			// 3. Nhận được ResultSet chứa các record thỏa điều kiện truy xuất
			ResultSet re = pre_control.executeQuery();
			int id;
			String filename = null;

			// 4. chạy từng record trong resultset
			while (re.next()) {

				id = re.getInt("id");
				String encode = re.getString("encode");

//				String valuesList = re.getString("insert_staging");// valuesList ? ?
				String table_staging = re.getString("table_staging_load");// table cua nhom
				String dir = re.getString("download_to_dir_local");
				filename = re.getString("your_filename");

				String delimiter = re.getString("delimiter");// dau phan cac cac phan tu
				int number_column = re.getInt("number_column");// so cot

				// 5. Kiểm tra file có tồn tại trên folder local "Data_Warehouse" chưa
				String path =   dir + "\\" + filename;
				System.out.println(path);
				File file = new File(path);// mo file
				if (!file.exists()) {
					// 6.1.1. Thông báo file không tồn tại ra màn hình
					System.out.println(path + "khong ton tai");
					 textSendMail = path + "khong ton tai";
						sendMail.sendMail("giabui21@gmail.com", "File bi bien mat ", textSendMail);
					// 6.1.2. Cập nhật status_file là ERROR Staging, time_staging là ngày giờ hiện
					// tại
					String sql2 = "UPDATE data_file_logs SET status_file='ERROR Staging', "
							+ "data_file_logs.time_staging=now() WHERE id=" + id;
					pre_control = conn.prepareStatement(sql2);
					pre_control.executeUpdate();
				} else {
					// neu file da ton tai
					Connection conn_Staging = new GetConnection().getConnection("staging?serverTimezone=UTC");
					int count = 0;// dem so dong doc duoc vao staging cua nhom
					// 1 mo connection cua staging

					// 2. thuc hien bulk vs table cua tung nhom
//					String sql = "bulk\r\n" + "INSERT " + table_staging + "\r\n" + "FROM '" + "D:\\" + dir + "\\"
//							+ filename + "'\r\n" + "WITH\r\n" + "(" + "FIRSTROW = 2,\r\n" + "FIELDTERMINATOR = '"
//							+ delimiter + "',\r\n" + "ROWTERMINATOR = '\\n'" + ")";

	
					 String sql = "LOAD DATA INFILE '" + dir + "\\" + filename + "' \r\n"
					 + "INTO TABLE "
					 + table_staging + "\r\n" + "FIELDS TERMINATED BY '" + delimiter + "' \r\n"
					 + "ENCLOSED BY '\"'\r\n" + "LINES TERMINATED BY '\\n'\r\n" + "IGNORE 1 ROWS";
					
					 try {
					 PreparedStatement pre_StagingAdd = conn_Staging.prepareStatement(sql);
					 count = pre_StagingAdd.executeUpdate();
					 System.out.println("Thanh Cong:\t" + "file name: " + filename + " ==> So dong thanh cong: " + count);
					 textSendMail = "Thanh Cong:\t" + "file name: " + filename + " ==> So dong thanh cong: " + count;
						sendMail.sendMail("giabui21@gmail.com", "Các file đã insert thành công vào db control", textSendMail);
					 
					 } catch (Exception e) {
						 System.out.println("Loi:\t" + "file name: " + filename   );
						 System.out.println(e);
						 textSendMail = "Loi:\t" + "file name: " + filename +"\n"+  "loai loi"+e+"\n";
							sendMail.sendMail("giabui21@gmail.com", "Các file error", textSendMail);
					 }
//					 pre_StagingAdd.close();
//					 conn_Staging.close();
					// 3. thuc hien update cac thong so lien quan
					// 6.2.8. Kiểm tra sô dòng đọc được vào staging của file
					if (count > 0) {
						
						String sql2 = "UPDATE data_file_logs SET staging_load_count=" + count + ", "
								+ "status_file='OK Staging', data_file_logs.time_staging=now()  WHERE id=" + id;
						pre_control = conn.prepareStatement(sql2);
						pre_control.executeUpdate();

					} else {
						String sql2 = "UPDATE data_file_logs SET staging_load_count=" + count + ", "
								+ "status_file='ERROR Staging', data_file_logs.time_staging=now() WHERE id=" + id;
						pre_control = conn.prepareStatement(sql2);
						pre_control.executeUpdate();
					}

				} // else file exist end

			} // end while file

			// dong nguon control
			re.close();
			pre_control.close();
			conn.close();

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}



	

	public static void main(String[] args) {
		 new Staging().updateBulk();
//	new Staging().staging("data_file_logs.status_file like 'OK download'");
	}
}
