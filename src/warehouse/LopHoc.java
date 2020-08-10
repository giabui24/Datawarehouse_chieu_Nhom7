package warehouse;

public class LopHoc {
	String maLopHoc;
	String maMH;
	String namHoc;
	String idFile;
	public LopHoc(String maLopHoc, String maMH, String namHoc, String idFile) {
		super();
		this.maLopHoc = maLopHoc;
		this.maMH = maMH;
		this.namHoc = namHoc;
		this.idFile = idFile;
	}
	public String getMaLopHoc() {
		return maLopHoc;
	}
	public void setMaLopHoc(String maLopHoc) {
		this.maLopHoc = maLopHoc;
	}
	public String getMaMH() {
		return maMH;
	}
	public void setMaMH(String maMH) {
		this.maMH = maMH;
	}
	public String getNamHoc() {
		return namHoc;
	}
	public void setNamHoc(String namHoc) {
		this.namHoc = namHoc;
	}
	public String getIdFile() {
		return idFile;
	}
	public void setIdFile(String idFile) {
		this.idFile = idFile;
	}
	@Override
	public String toString() {
		return "LopHoc [maLopHoc=" + maLopHoc + ", maMH=" + maMH + ", namHoc=" + namHoc + ", idFile=" + idFile + "]";
	}
	

}
