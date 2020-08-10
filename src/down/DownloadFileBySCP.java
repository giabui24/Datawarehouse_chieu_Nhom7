package down;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.xml.bind.annotation.XmlElementDecl.GLOBAL;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

import config.Config;
import connection.ConnectMysql;
import warehouse.ImportDangKyToWH;
import warehouse.ImportLopHocToWH;
import warehouse.ImportMonHocToWH;
import warehouse.ImportSinhVienToWH;
import warehouse.LoadConfig;
import etl.Staging;
import log.Log;

public class DownloadFileBySCP {
	public static Staging stagig = new Staging();
	public static DownloadFileBySCP scpObject = new DownloadFileBySCP();
	public Config config = new Config(null, null, null, null, null, null, null,null,null,null,null,null,null,null,null,null,null,null);
	public Log log = new Log(null, null, null, null, null, null, null, null, null, null);
	String text = "";
	public String folderToDown = "Z:\\data_WH\\";
	public String tempfolder = "Z:\\viet\\";

	static {
		try {
//			System.loadLibrary("chilkat");
			System.load("T:\\A_TheDuck_viet\\warehouse\\chilkat-9.5.0-jdk8-x64\\chilkat.dll");
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}

	public void connectToConfig(String idConfig) throws AddressException, MessagingException {
		try {
			Connection connectDB = ConnectMysql.getConnection();

			String sql = "Select idconfig, hostname, port, user,password,remotepath, namesub , stagingload from config where idconfig ='"
					+ idConfig + "'";

			Statement statement;

			statement = connectDB.createStatement();

			ResultSet rs = statement.executeQuery(sql);
			while (rs.next()) {// Di chuyển con trỏ xuống bản ghi kế tiếp.
				config.setIdconfig(rs.getString(1));
				config.setHostname(rs.getString(2));
				config.setPort(rs.getString(3));
				config.setUser(rs.getString(4));
				config.setPassword(rs.getString(5));
				config.setRemotepath(rs.getString(6));
				config.setNamesub(rs.getString(7));
				config.setStagingload(rs.getString(8));

			}
			// Đóng kết nối
			connectDB.close();
			config.toString();
		} catch (Exception e) {
			scpObject.sendMail("Connect ERROR");

			System.out.println("error statement");
			e.printStackTrace();
		}

	}

	public void createTempFolderToDownload() {
		File file = new File(tempfolder);
		if (!file.exists()) {
			file.mkdir();
			System.out.println("Directory is created!");
		} else if (file.list().length == 0) {
			System.out.println("Directory is existed, empty and ready to use!");
		} else {
			System.out.println("Failed to create directory use to download!");
		}
	}

	public void deleteFolder() {
		File file = new File(tempfolder);
//		file.delete();
//		System.out.println("Folder used to download was deleted");
		// neu file la thu muc thi xoa het thu muc con va file cua no
		if (file.isDirectory()) {
			// liet ke tat ca file
			String[] files = file.list();
			for (String child : files) {
				File childDir = new File(file, child);
				// neu childDir la file thi xoa
				childDir.delete();
				System.out.println("Files were deleted : " + childDir.getAbsolutePath());

			}
			// Check lai va xoa thu muc cha
			if (file.list().length == 0) {
				file.delete();
				System.out.println(" Folder was deleted : " + file.getAbsolutePath());
			}

		} else {
			// neu file la file thi xoa
			file.delete();
			System.out.println("File bi da bi xoa " + file.getAbsolutePath());
		}

	}

	public static void download() {

		// lấy thông tin từ config

		String hostname = scpObject.config.getHostname();
		int port = Integer.parseInt(scpObject.config.getPort());
		String username = scpObject.config.getUser();
		String pass = scpObject.config.getPassword();
		String remotepath = scpObject.config.getRemotepath();
		String namesub = scpObject.config.getNamesub();
		// tạo thư mục để lưu tạm
		scpObject.createTempFolderToDownload();
		String localPath = scpObject.tempfolder;

		CkSsh ssh = new CkSsh();

		// unclock .....
		CkGlobal ck = new CkGlobal();
		GLOBAL glo = new GLOBAL();
		ck.UnlockBundle("Start my 30-day Trial");

		ck.get_UnlockStatus();

		// Connect to an SSH server:
		boolean success = ssh.Connect(hostname, port);
		if (!success) {
			System.out.println(ssh.lastErrorText());
			return;
		}

		// Wait a max of 5 seconds when reading responses..
		ssh.put_IdleTimeoutMs(5000);

		// Authenticate using login/password:
		success = ssh.AuthenticatePw(username, pass);
		if (!success) {
			System.out.println(ssh.lastErrorText());
			return;
		}
		CkScp scp = new CkScp();
		success = scp.UseSsh(ssh);
		if (!success) {
			System.out.println(ssh.lastErrorText());
			return;
		}

		// download directory chieu
		scp.put_SyncMustMatch(namesub);
		success = scp.SyncTreeDownload(remotepath, localPath, 2, false);
	
		if (!success) {
			System.out.println(ssh.lastErrorText());
			return;
		}

		System.out.println("SCP download file success.");
		ssh.Disconnect();

	}

// gửi gmail
	public void sendMail(String text) throws AddressException, MessagingException {
		Properties p = new Properties();
		p.put("mail.smtp.auth", "true");
		p.put("mail.smtp.starttls.enable", "true");
		p.put("mail.smtp.host", "smtp.gmail.com");
		p.put("mail.smtp.port", 587);
		// get Session
		Session s = Session.getInstance(p, new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication("vietnguyen.zuyn01@gmail.com", "netingepytkngqut");
			}
		});
		Message msg = new MimeMessage(s);
		msg.setFrom(new InternetAddress("vietnguyen.zuyn@gmail.com"));
		msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse("vietnguyen.zuyn04@gmail.com"));
		msg.setSubject("Save Log");
		msg.setText(text);
		System.out.println("send gmail done");
		Transport.send(msg);
	}

// viết cập nhật vào log
	public void writeLog(File file) throws AddressException, MessagingException {
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
		Date date = new Date();
		String dateFormat = formatter.format(date);

		try {
			Connection connectControlDB = ConnectMysql.getConnection();
			String sql = "INSERT INTO data_file_logs ( your_filename, status_file,encode,delimiter,number_column,download_to_dir_local,time_staging,staging_load_count,table_staging_load) VALUES (?,?,?,?,?,?,?,?,?)";
			PreparedStatement statement = connectControlDB.prepareStatement(sql);

			statement.setString(1, file.getName());
			statement.setString(2, "OK download");
			statement.setString(3, "UTF-8");
			statement.setString(4, ";"); // delimiter
			statement.setString(5, "");// column
			statement.setString(6, folderToDown);
			statement.setString(7, "");// time staging
			statement.setString(8, "");// staging count
			statement.setString(9, config.getStagingload());// table staging
			// System.out.println(Long.toString(file.length()));

			statement.executeUpdate();
			connectControlDB.close();
			System.out.println("write log: " + file.getName());

			System.out.println("**********");
		} catch (Exception e) {
			System.out.println("fail write Log");
			scpObject.sendMail("Write Log Fail");
			System.out.println(e);
		}

	}

	public void transTemporaryFolder(String pathOfStagingFolder) throws AddressException, MessagingException {
		File usedFolder = new File(tempfolder);
		File[] childFile = usedFolder.listFiles();
		int count = 0;
		for (File file : childFile) {
			if (file.renameTo(new File(folderToDown + file.getName()))) {
				// scpObject.writeLog(file);
				System.out.println(file.getName() + " is moved successful!");
				text += file.getName() + " ==> put log complete " + "\n";
				count++;
			} else {
				System.out.println(file.getName() + " is failed to move!");
				text += file.getName() + " ==>put log error " + "\n";
			}
		}
		scpObject.deleteFolder();
		scpObject.sendMail(text + "\n" + count + " file done");

	}




	// cập nhật vào log
	public void writeLogFollder() throws AddressException, MessagingException {
		File dir = new File(folderToDown);

		File[] children = dir.listFiles();
//
		for (File files : children) {
			scpObject.writeLog(files);

		}
	}





	public void mainSCP(String idconfig) throws AddressException, MessagingException, SQLException, ClassNotFoundException {
		scpObject.connectToConfig(idconfig);
//		scpObject.download();
//		scpObject.transTemporaryFolder(folderToDown);
		scpObject.writeLogFollder();
		stagig.updateBulk();
//
		ArrayList<warehouse.Config> list = new LoadConfig().loadConfig();
		warehouse.Config config;
	//	for (int i= 1; i<=list.size(); i++){
		int id = Integer.parseInt(idconfig);
			config = list.get(id);
			String file = config.getNamesub();
			if (file.substring(0, 8).equals("sinhvien"))
				new ImportSinhVienToWH().importSV(id);
			if (file.substring(0, 6).equals("Monhoc"))
				new ImportMonHocToWH().importMH(id);
			if (file.substring(0, 6).equals("Lophoc"))
				new ImportLopHocToWH().importLH(id);
			if (file.substring(0, 6).equals("Dangky"))
				new ImportDangKyToWH().importDK(id);
		
	}

//	public static void main(String[] args) throws AddressException, MessagingException, ClassNotFoundException, SQLException {
//		DownloadFileBySCP d = new DownloadFileBySCP();
//		
//		 d.mainSCP(args[0]);
	//	d.mainSCP("2");
//	}
}
