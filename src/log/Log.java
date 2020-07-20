package log;

public class Log {
	private String id;
	private String your_filename;
	private String status_file;
	private String encode;
	private String delimiter;
	private String number_column;
	private String download_to_dir_local;
	private String time_staging;
	private String staging_load_count;
	private String table_staging_load;
	public Log(String id, String your_filename, String status_file, String encode, String delimiter,
			String number_column, String download_to_dir_local, String time_staging, String staging_load_count,
			String table_staging_load) {
		super();
		this.id = id;
		this.your_filename = your_filename;
		this.status_file = status_file;
		this.encode = encode;
		this.delimiter = delimiter;
		this.number_column = number_column;
		this.download_to_dir_local = download_to_dir_local;
		this.time_staging = time_staging;
		this.staging_load_count = staging_load_count;
		this.table_staging_load = table_staging_load;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getYour_filename() {
		return your_filename;
	}
	public void setYour_filename(String your_filename) {
		this.your_filename = your_filename;
	}
	public String getStatus_file() {
		return status_file;
	}
	public void setStatus_file(String status_file) {
		this.status_file = status_file;
	}
	public String getEncode() {
		return encode;
	}
	public void setEncode(String encode) {
		this.encode = encode;
	}
	public String getDelimiter() {
		return delimiter;
	}
	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}
	public String getNumber_column() {
		return number_column;
	}
	public void setNumber_column(String number_column) {
		this.number_column = number_column;
	}
	public String getDownload_to_dir_local() {
		return download_to_dir_local;
	}
	public void setDownload_to_dir_local(String download_to_dir_local) {
		this.download_to_dir_local = download_to_dir_local;
	}
	public String getTime_staging() {
		return time_staging;
	}
	public void setTime_staging(String time_staging) {
		this.time_staging = time_staging;
	}
	public String getStaging_load_count() {
		return staging_load_count;
	}
	public void setStaging_load_count(String staging_load_count) {
		this.staging_load_count = staging_load_count;
	}
	public String getTable_staging_load() {
		return table_staging_load;
	}
	public void setTable_staging_load(String table_staging_load) {
		this.table_staging_load = table_staging_load;
	}
	
}
