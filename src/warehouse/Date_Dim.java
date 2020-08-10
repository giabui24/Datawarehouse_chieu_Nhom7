package warehouse;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.StringTokenizer;
import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.Period;

public class Date_Dim {
	public static final String OUT_FILE = "date_dim_without_quarter2.csv";
	public static final int NUMBER_OF_RECORD = 1;
	public static final String TIME_ZONE = "PST8PDT";

	public int getSKDateDim(String dateTime) throws SQLException {
		ConnectDatabase conDB = new ConnectDatabase();
		Connection con=null;
		try{
		con = conDB.connectDateDim();
		}
		catch (Exception e) {
			// TODO: handle exception
		}
		Statement sta = con.createStatement();
		String sql = "Select * from datedim where full_date = '" + dateTime + "'";
		ResultSet re = sta.executeQuery(sql);
		int sk = 0;
		if (re.next()) {
			sk = Integer.valueOf(re.getString("id"));
		}
		con.close();
		return sk;
	}
	public void insertDateDim(String dateTime) throws ClassNotFoundException, SQLException{
		StringTokenizer ns = new StringTokenizer(dateTime,"/");
		int y = Integer.valueOf(ns.nextToken());
		int m = Integer.valueOf(ns.nextToken());
		int d= Integer.valueOf(ns.nextToken());
		DateTime dt=new DateTime(y,m,d,0,0,0);
		importDateDim(dt, dt.plus(Period.days(1)));
	}

	public void importDateDim(DateTime startDateTime, DateTime endDateTime) throws ClassNotFoundException, SQLException {
		ConnectDatabase conDB = new ConnectDatabase();
		Connection con = conDB.connectDateDim();
		int sk;
		String full_date = "";
		String day_of_week = "";
		String calendar_month = "";
		String calendar_year = "";
		String calendar_year_month = "";
		int day_of_month = 0;
		int day_of_year = 0;
		int week_of_year_sunday = 0;
		String year_week_sunday = "";
		String week_sunday_start = "";
		int week_of_year_monday = 0;
		String year_week_monday = "";
		String day_type = "";
		while (!startDateTime.equals(endDateTime)) {
			startDateTime = startDateTime.plus(Period.days(1));
			Date startDate = startDateTime.plus(Period.days(5)).toDate();
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(startDate);

			SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd");
			// Full Date
			full_date = dt.format(calendar.getTime());

			// Day of Week
			day_of_week = calendar.getDisplayName(Calendar.DAY_OF_WEEK, Calendar.LONG, Locale.US);

			// Calendar Month
			calendar_month = calendar.getDisplayName(Calendar.MONTH, Calendar.LONG, Locale.US);

			dt = new SimpleDateFormat("yyyy");
			// Calendar Year
			calendar_year = dt.format(calendar.getTime());
			String calendar_month_short = calendar.getDisplayName(Calendar.MONTH, Calendar.SHORT, Locale.US);

			// Calendar Year Month
			calendar_year_month = calendar_year + "-" + calendar_month_short;

			// Date of Month
			day_of_month = calendar.get(Calendar.DAY_OF_MONTH);

			// Day of Year
			day_of_year = calendar.get(Calendar.DAY_OF_YEAR);
			Calendar calendar_temp = calendar;

			// Week of Year Sunday
			week_of_year_sunday = lastDayOfLastWeek(calendar_temp).get(Calendar.WEEK_OF_YEAR);
			int year_sunday = lastDayOfLastWeek(calendar).get(Calendar.YEAR);

			// Year Week Sunday
			year_week_sunday = "";
			if (week_of_year_sunday < 10) {
				year_week_sunday = year_sunday + "-" + "W0" + week_of_year_sunday;
			} else {
				year_week_sunday = year_sunday + "-" + "W" + week_of_year_sunday;
			}
			calendar_temp = Calendar.getInstance(Locale.US);
			calendar_temp.setTime(calendar.getTime());
			calendar_temp.set(Calendar.DAY_OF_WEEK, calendar_temp.getFirstDayOfWeek());
			dt = new SimpleDateFormat("yyyy-MM-dd");
			// Week Sunday Start
			week_sunday_start = dt.format(calendar_temp.getTime()); // 13
			DateTime startOfWeek = startDateTime.weekOfWeekyear().roundFloorCopy();
			// Week of Year Monday
			week_of_year_monday = startOfWeek.getWeekOfWeekyear(); // 14
			dt = new SimpleDateFormat("yyyy");
			int year_week_monday_temp = startOfWeek.getYear();
			// Year Week Monday
			year_week_monday = "";
			if (week_of_year_monday < 10) {
				year_week_monday = year_week_monday_temp + "-W0" + week_of_year_monday;
			} else {
				year_week_monday = year_week_monday_temp + "-W" + week_of_year_monday;
			}
			dt = new SimpleDateFormat("yyyy-MM-dd");
			// Day Type
			day_type = isWeekend(day_of_week); // 18

			sk = 0;
			sk = this.getSKDateDim(startDateTime.toString().substring(0, 10));
			if (sk == 0) {
				String sql = "Insert into datedim (full_date,day_of_week,calendar_month,calendar_year,calendar_year_month,"
						+ "day_of_month,day_of_year,week_of_year_sunday,year_week_sunday,week_sunday_start,week_of_year_monday,"
						+ "year_week_monday,day_type) values('" + full_date + "','" + day_of_week + "','"
						+ calendar_month + "','" + calendar_year + "','" + calendar_year_month + "',"
						+ String.valueOf(day_of_month) + "," + String.valueOf(day_of_year) + ","
						+ String.valueOf(week_of_year_sunday) + ",'" + year_week_sunday + "','" + week_sunday_start
						+ "'," + week_of_year_monday + ",'" + year_week_monday + "','" + day_type + "')";
				Statement sta = con.createStatement();
				sta.execute(sql);
			}
		}
		con.close();
	}

	public static Calendar lastDayOfLastWeek(Calendar c) {
		c = (Calendar) c.clone();
		// first day of this week
		c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek());
		// last day of previous week
		c.add(Calendar.DAY_OF_MONTH, -1);
		return c;
	}

	// Check if Given day is weekend (Saturday or Sunday)
	public static String isWeekend(String day) {
		if (day.equalsIgnoreCase("Saturday") || day.equalsIgnoreCase("Sunday")) {
			return "Weekend";
		} else {
			return "Weekday";
		}
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Date_Dim dim = new Date_Dim();
		
		dim.insertDateDim("1003/3/16");
		System.out.println(dim.getSKDateDim("1003/3/16"));
	}
}
