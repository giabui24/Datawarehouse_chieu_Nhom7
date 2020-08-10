package testmain;

import java.sql.SQLException;

import javax.mail.MessagingException;
import javax.mail.internet.AddressException;

import down.DownloadFileBySCP;

public class RunMain {
public static void main(String[] args) throws AddressException, ClassNotFoundException, MessagingException, SQLException {
	DownloadFileBySCP down = new DownloadFileBySCP();
	down.mainSCP(args[0]);
}
}
