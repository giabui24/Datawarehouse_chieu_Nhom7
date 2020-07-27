package warehouse;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.Period;

public class Date_Dim {
	public static final String OUT_FILE = "date_dim_without_quarter2.csv";
	public static final int NUMBER_OF_RECORD = 1;
	public static final String TIME_ZONE = "PST8PDT";

	public int getSKDateDim(String dateTime) throws ClassNotFoundException, SQLException {
		ConnectDatabase conDB = new ConnectDatabase();
		Connection con = conDB.connectDateDim();
		Statement sta = con.createStatement();
		String sql = "Select * from datedim where datedim.full_date = '" + dateTime + "'";
		ResultSet re = sta.executeQuery(sql);
		int sk = 0;
		if (re.next()) {
			sk = Integer.valueOf(re.getString("id"));
		}
		con.close();
		return sk;
	}

	public void importDateDim(DateTime dateTime) throws ClassNotFoundException, SQLException {
		int sk = 0;
		sk = this.getSKDateDim(dateTime.toString().substring(0, 10));
		if (sk == 0) {
			ConnectDatabase conDB = new ConnectDatabase();
			Connection con = conDB.connectDateDim();
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

			DateTime startDateTime = new DateTime(1995, 12, 31, 0, 0, 0);
			while (!startDateTime.equals(dateTime)) {
				startDateTime = startDateTime.plus(Period.days(1));
				Date startDate = startDateTime.toDate();
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
			}
			String sql = "Insert into datedim (full_date,day_of_week,calendar_month,calendar_year,calendar_year_month,"
					+ "day_of_month,day_of_year,week_of_year_sunday,year_week_sunday,week_sunday_start,week_of_year_monday,"
					+ "year_week_monday,day_type) values('" + full_date + "','" + day_of_week + "','" + calendar_month
					+ "','" + calendar_year + "','" + calendar_year_month + "'," + String.valueOf(day_of_month) + ","
					+ String.valueOf(day_of_year) + "," + String.valueOf(week_of_year_sunday) + ",'" + year_week_sunday
					+ "','" + week_sunday_start + "'," + week_of_year_monday + ",'" + year_week_monday + "','"
					+ day_type + "')";
			Statement sta = con.createStatement();
			sta.execute(sql);
			con.close();
		}
	}

	public static String getWeekOfYearSunday(Calendar calendar) {
		Date date = getFirstDayOfWeekDate(calendar);
		Calendar newCalendar = Calendar.getInstance(Locale.US);
		newCalendar.setTime(date);
		int result = newCalendar.getWeeksInWeekYear();
		return "" + result;
	}

	public static String getFirstDayOfWeekString(Calendar calendar) {
		int week = calendar.get(Calendar.DAY_OF_WEEK);
		SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd");
		Date now = calendar.getTime();
		Date temp = new Date(now.getTime() - 24 * 60 * 60 * 1000 * (week - 1));
		String result = dt.format(temp);
		return result;
	}

	public static Date getFirstDayOfWeekDate(Calendar calendar) {
		int week = calendar.get(Calendar.DAY_OF_WEEK);
		Date now = calendar.getTime();
		Date temp = new Date(now.getTime() - 24 * 60 * 60 * 1000 * (week - 1));
		return temp;
	}

	public static Calendar getDateOfMondayInCurrentWeek(Calendar c) {
		c.setFirstDayOfWeek(Calendar.MONDAY);
		int today = c.get(Calendar.DAY_OF_WEEK);
		c.add(Calendar.DAY_OF_WEEK, -today + Calendar.MONDAY);
		return c;
	}

	public static Calendar firstDayOfLastWeek(Calendar c) {
		c = (Calendar) c.clone();
		// last week
		c.add(Calendar.WEEK_OF_YEAR, -1);
		// first day
		c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek());
		return c;
	}

	public static Calendar lastDayOfLastWeek(Calendar c) {
		c = (Calendar) c.clone();
		// first day of this week
		c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek());
		// last day of previous week
		c.add(Calendar.DAY_OF_MONTH, -1);
		return c;
	}

	/*
	 * Check if Given day is weekend (Saturday or Sunday)
	 */
	public static String isWeekend(String day) {
		if (day.equalsIgnoreCase("Saturday") || day.equalsIgnoreCase("Sunday")) {
			return "Weekend";
		} else {
			return "Weekday";
		}
	}

	/**
	 * 
	 */
	public static String getQuarter(int month) {
		int quarter = month % 3 == 0 ? (month / 3) : (month / 3) + 1;
		String result = "Q" + quarter;
		return result;
	}
}
