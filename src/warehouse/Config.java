
public class Config {
	String idConfig;
	String hostName;
	String port;
	String user;
	String password;
	String remotepath;
	String namesub;
	String sVNameSta;
	String dBNameSta;
	String tBNameSta;
	String userNameSta;
	String passSta;
	String sVNameWH;
	String dBNameWH;
	String tBNameWH;
	String userNameWH;
	String passWH;
	public Config(String idConfig, String hostName, String port, String user, String password, String remotepath,
			String namesub, String sVNameSta, String dBNameSta, String tBNameSta, String userNameSta, String passSta,
			String sVNameWH, String dBNameWH, String tBNameWH, String userNameWH, String passWH) {
		super();
		this.idConfig = idConfig;
		this.hostName = hostName;
		this.port = port;
		this.user = user;
		this.password = password;
		this.remotepath = remotepath;
		this.namesub = namesub;
		this.sVNameSta = sVNameSta;
		this.dBNameSta = dBNameSta;
		this.tBNameSta = tBNameSta;
		this.userNameSta = userNameSta;
		this.passSta = passSta;
		this.sVNameWH = sVNameWH;
		this.dBNameWH = dBNameWH;
		this.tBNameWH = tBNameWH;
		this.userNameWH = userNameWH;
		this.passWH = passWH;
	}
	public String getIdConfig() {
		return idConfig;
	}
	public void setIdConfig(String idConfig) {
		this.idConfig = idConfig;
	}
	public String getHostName() {
		return hostName;
	}
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getRemotepath() {
		return remotepath;
	}
	public void setRemotepath(String remotepath) {
		this.remotepath = remotepath;
	}
	public String getNamesub() {
		return namesub;
	}
	public void setNamesub(String namesub) {
		this.namesub = namesub;
	}
	public String getsVNameSta() {
		return sVNameSta;
	}
	public void setsVNameSta(String sVNameSta) {
		this.sVNameSta = sVNameSta;
	}
	public String getdBNameSta() {
		return dBNameSta;
	}
	public void setdBNameSta(String dBNameSta) {
		this.dBNameSta = dBNameSta;
	}
	public String gettBNameSta() {
		return tBNameSta;
	}
	public void settBNameSta(String tBNameSta) {
		this.tBNameSta = tBNameSta;
	}
	public String getUserNameSta() {
		return userNameSta;
	}
	public void setUserNameSta(String userNameSta) {
		this.userNameSta = userNameSta;
	}
	public String getPassSta() {
		return passSta;
	}
	public void setPassSta(String passSta) {
		this.passSta = passSta;
	}
	public String getsVNameWH() {
		return sVNameWH;
	}
	public void setsVNameWH(String sVNameWH) {
		this.sVNameWH = sVNameWH;
	}
	public String getdBNameWH() {
		return dBNameWH;
	}
	public void setdBNameWH(String dBNameWH) {
		this.dBNameWH = dBNameWH;
	}
	public String gettBNameWH() {
		return tBNameWH;
	}
	public void settBNameWH(String tBNameWH) {
		this.tBNameWH = tBNameWH;
	}
	public String getUserNameWH() {
		return userNameWH;
	}
	public void setUserNameWH(String userNameWH) {
		this.userNameWH = userNameWH;
	}
	public String getPassWH() {
		return passWH;
	}
	public void setPassWH(String passWH) {
		this.passWH = passWH;
	}
	@Override
	public String toString() {
		return "Config [idConfig=" + idConfig + ", hostName=" + hostName + ", port=" + port + ", user=" + user
				+ ", password=" + password + ", remotepath=" + remotepath + ", namesub=" + namesub + ", sVNameSta="
				+ sVNameSta + ", dBNameSta=" + dBNameSta + ", tBNameSta=" + tBNameSta + ", userNameSta=" + userNameSta
				+ ", passSta=" + passSta + ", sVNameWH=" + sVNameWH + ", dBNameWH=" + dBNameWH + ", tBNameWH="
				+ tBNameWH + ", userNameWH=" + userNameWH + ", passWH=" + passWH + "]";
	}
	
	
}
