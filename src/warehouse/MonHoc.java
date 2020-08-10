package warehouse;

public class MonHoc {
	String stt;
	String maMH;
	String tenMH;
	String tc;
	String khoaBMQuanLy;
	String khoaBMSuDung;
	String idFile;
	public MonHoc(String stt, String maMH, String tenMH, String tc, String khoaBMQuanLy, String khoaBMSuDung,
			String idFile) {
		super();
		this.stt = stt;
		this.maMH = maMH;
		this.tenMH = tenMH;
		this.tc = tc;
		this.khoaBMQuanLy = khoaBMQuanLy;
		this.khoaBMSuDung = khoaBMSuDung;
		this.idFile = idFile;
	}
	public String getStt() {
		return stt;
	}
	public void setStt(String stt) {
		this.stt = stt;
	}
	public String getMaMH() {
		return maMH;
	}
	public void setMaMH(String maMH) {
		this.maMH = maMH;
	}
	public String getTenMH() {
		return tenMH;
	}
	public void setTenMH(String tenMH) {
		this.tenMH = tenMH;
	}
	public String getTc() {
		return tc;
	}
	public void setTc(String tc) {
		this.tc = tc;
	}
	public String getKhoaBMQuanLy() {
		return khoaBMQuanLy;
	}
	public void setKhoaBMQuanLy(String khoaBMQuanLy) {
		this.khoaBMQuanLy = khoaBMQuanLy;
	}
	public String getKhoaBMSuDung() {
		return khoaBMSuDung;
	}
	public void setKhoaBMSuDung(String khoaBMSuDung) {
		this.khoaBMSuDung = khoaBMSuDung;
	}
	public String getIdFile() {
		return idFile;
	}
	public void setIdFile(String idFile) {
		this.idFile = idFile;
	}
	
	public String toString(){
		return stt+"\t"+maMH+"\t"+tenMH+"\t"+tc+"\t"+khoaBMQuanLy+"\t"+khoaBMSuDung;
	}
}