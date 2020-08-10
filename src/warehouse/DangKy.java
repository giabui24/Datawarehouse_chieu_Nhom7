package warehouse;


public class DangKy {
	String maDK;
	String maSV;
	String maLopHoc;
	String thoiGianDK;
	String idFile;
	public DangKy(String maDK, String maSV, String maLopHoc, String thoiGianDK, String idFile) {
		super();
		this.maDK = maDK;
		this.maSV = maSV;
		this.maLopHoc = maLopHoc;
		this.thoiGianDK = thoiGianDK;
		this.idFile = idFile;
	}
	public String getMaDK() {
		return maDK;
	}
	public void setMaDK(String maDK) {
		this.maDK = maDK;
	}
	public String getMaSV() {
		return maSV;
	}
	public void setMaSV(String maSV) {
		this.maSV = maSV;
	}
	public String getMaLopHoc() {
		return maLopHoc;
	}
	public void setMaLopHoc(String maLopHoc) {
		this.maLopHoc = maLopHoc;
	}
	public String getThoiGianDK() {
		return thoiGianDK;
	}
	public void setThoiGianDK(String thoiGianDK) {
		this.thoiGianDK = thoiGianDK;
	}
	public String getIdFile() {
		return idFile;
	}
	public void setIdFile(String idFile) {
		this.idFile = idFile;
	}
	@Override
	public String toString() {
		return "DangKy [maDK=" + maDK + ", maSV=" + maSV + ", maLopHoc=" + maLopHoc + ", thoiGianDK=" + thoiGianDK
				+ ", idFile=" + idFile + "]";
	}
	

}
