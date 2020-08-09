
public class SinhVien {
	String maSV;
	String ho;
	String ten;
	String ngaySinh;
	String maLop;
	String tenLop;
	String dt;
	String email;
	String queQuan;
	String idFile;
	public SinhVien(String maSV, String ho, String ten, String ngaySinh, String maLop, String tenLop, String dt,
			String email, String queQuan, String idFile) {
		super();
		this.maSV = maSV;
		this.ho = ho;
		this.ten = ten;
		this.ngaySinh = ngaySinh;
		this.maLop = maLop;
		this.tenLop = tenLop;
		this.dt = dt;
		this.email = email;
		this.queQuan = queQuan;
		this.idFile = idFile;
	}
	public String getMaSV() {
		return maSV;
	}
	public void setMaSV(String maSV) {
		this.maSV = maSV;
	}
	public String getHo() {
		return ho;
	}
	public void setHo(String ho) {
		this.ho = ho;
	}
	public String getTen() {
		return ten;
	}
	public void setTen(String ten) {
		this.ten = ten;
	}
	public String getNgaySinh() {
		return ngaySinh;
	}
	public void setNgaySinh(String ngaySinh) {
		this.ngaySinh = ngaySinh;
	}
	public String getMaLop() {
		return maLop;
	}
	public void setMaLop(String maLop) {
		this.maLop = maLop;
	}
	public String getTenLop() {
		return tenLop;
	}
	public void setTenLop(String tenLop) {
		this.tenLop = tenLop;
	}
	public String getDt() {
		return dt;
	}
	public void setDt(String dt) {
		this.dt = dt;
	}
	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	public String getQueQuan() {
		return queQuan;
	}
	public void setQueQuan(String queQuan) {
		this.queQuan = queQuan;
	}
	public String getIdFile() {
		return idFile;
	}
	public void setIdFile(String idFile) {
		this.idFile = idFile;
	}
	@Override
	public String toString() {
		return "SinhVien [maSV=" + maSV + ", ho=" + ho + ", ten=" + ten + ", ngaySinh=" + ngaySinh + ", maLop=" + maLop
				+ ", tenLop=" + tenLop + ", dt=" + dt + ", email=" + email + ", queQuan=" + queQuan + ", idFile="
				+ idFile + "]";
	}
	
}
