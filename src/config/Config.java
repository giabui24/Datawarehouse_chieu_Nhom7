package config;

import java.sql.Date;

public class Config {
	private String idconfig;
	private String hostname;
	private String port;
	private String user;
	private String password;
	private String remotepath;
	private String namesub;
	private String stagingload;
	public Config(String idconfig, String hostname, String port, String user, String password, String remotepath,
			String namesub, String stagingload) {
		super();
		this.idconfig = idconfig;
		this.hostname = hostname;
		this.port = port;
		this.user = user;
		this.password = password;
		this.remotepath = remotepath;
		this.namesub = namesub;
		this.stagingload = stagingload;
	}
	public String getIdconfig() {
		return idconfig;
	}
	public void setIdconfig(String idconfig) {
		this.idconfig = idconfig;
	}
	public String getHostname() {
		return hostname;
	}
	public void setHostname(String hostname) {
		this.hostname = hostname;
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
	public String getStagingload() {
		return stagingload;
	}
	public void setStagingload(String stagingload) {
		this.stagingload = stagingload;
	}
	

}