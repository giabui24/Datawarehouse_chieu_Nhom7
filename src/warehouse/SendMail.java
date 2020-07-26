package warehouse;
import java.util.Properties;

import javax.mail.*;
import javax.mail.internet.*;

public class SendMail {
	public static void send(String msg) {
		String sub = "Thông báo từ hệ thống Datawarehouse";
		final String user = "ringsstorenl@gmail.com";
		final String pass = "ringsstore123";

		Properties props = new Properties();

		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");

		Session session = Session.getInstance(props, new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(user, pass);
			}
		});

		try {

			// Create an instance of MimeMessage, it accept MIME types and
			// headers

			MimeMessage message = new MimeMessage(session);
			message.setFrom(new InternetAddress(user));
			message.addRecipient(Message.RecipientType.TO, new InternetAddress("17130152@st.hcmuaf.edu.vn"));
			message.setSubject(sub);
			message.setText(msg);

			/*
			 * Transport class is used to deliver the message to the recipients
			 */

			Transport.send(message);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
