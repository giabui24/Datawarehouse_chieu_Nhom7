package warehouse;
import java.sql.SQLException;
import java.util.ArrayList;

public class Test {
	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		ArrayList<Config> list = new LoadConfig().loadConfig();
		Config config;
		for (int i= 1; i<=list.size(); i++){
			config = list.get(i-1);
			String file = config.getNamesub();
			if (file.substring(0, 8).equals("sinhvien"))
				new ImportSinhVienToWH().importSV(i);
			if (file.substring(0, 6).equals("Monhoc"))
				new ImportMonHocToWH().importMH(i);
			if (file.substring(0, 6).equals("Lophoc"))
				new ImportLopHocToWH().importLH(i);
			if (file.substring(0, 6).equals("Dangky"))
				new ImportDangKyToWH().importDK(i);
		}
	}
}
