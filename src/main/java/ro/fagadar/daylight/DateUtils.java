package ro.fagadar.daylight;

import java.sql.Timestamp;
import java.util.Date;

import com.google.gwt.i18n.client.DateTimeFormat;

@SuppressWarnings("deprecation")
public class DateUtils {

	private static final long MS_PER_SEC = 1000;
	private static final long MS_PER_MIN = MS_PER_SEC * 60;
	private static final long MS_PER_HOUR = MS_PER_MIN * 60;
	private static final long MS_PER_DAY = MS_PER_HOUR * 24;

	public static Date truncateToTime(Date date) {
		long time = date.getTime();
		return new Date(time - truncateToDay(date).getTime());
	}

	public static Date truncateToDay(Date date) {
		return new Date(date.getYear(), date.getMonth(), date.getDate());
	}

	public static Date truncateToMonth(Date date) {
		return addDays(truncateToDay(date), 1 - date.getDate());
	}

	public static Date truncateToYear(Date date) {
		date = truncateToMonth(date);
		date.setMonth(0);
		return date;
	}

	public static Date addMinutes(Date date, int minutes) {
		long t = date.getTime();
		t = t + minutes * MS_PER_MIN;
		return new Date(t);
	}

	public static Date addDays(Date date, int days) {
		Date newDate = new Date(date.getTime());
		newDate.setDate(date.getDate() + days);
		return newDate;
	}

	public static Date addMonths(Date date, int months) {
		for (; months < 0; months++) {
			Date roundedPriorMonth = addDays(truncateToMonth(date), -1);
			date = addDays(date, -getDaysInMonth(roundedPriorMonth));
		}
		for (; months > 0; months--) {
			date = addDays(date, getDaysInMonth(date));
		}
		return date;
	}

	public static int getDaysInMonth(Date date) {
		return getDaysInMonth(date.getYear() + 1900, date.getMonth());
	}

	public static int getDaysInMonth(int year, int month) {
		switch (month) {
		case 1:
			return (((year % 4) == 0 && (year % 100) != 0) || (year % 400) == 0) ? 29 : 28;

		case 3:
		case 5:
		case 8:
		case 10:
			return 30;

		default:
			return 31;
		}
	}

	/**
	 * Determines the number of days between two dates, always rounding up so a
	 * difference of 1 day 1 second yield a return value of 2.
	 */
	public static int dayDiff(Date endDate, Date startDate) {
		return (int) Math.ceil(((double) endDate.getTime() - startDate.getTime()) / MS_PER_DAY);
	}

	public static int hourDiff(Date endDate, Date startDate) {
		return (int) Math.ceil(((double) endDate.getTime() - startDate.getTime()) / MS_PER_HOUR);
	}

	public static int minDiff(Date endDate, Date startDate) {
		return (int) Math.ceil(((double) endDate.getTime() - (double) startDate.getTime()) / MS_PER_MIN);
	}

	public static String Date2String(Date date) {
		return Date2String(date, "yyyy-MM-dd");
	}

	public static String Date2String(Date date, String strFormat) {
		if (date == null)
			return "";
		try {
			// DateTimeFormat fmt = DateTimeFormat.getFormat("yyyy-MM-dd");
			DateTimeFormat fmt = DateTimeFormat.getFormat(strFormat);
			return fmt.format(date);
		} catch (Exception e) {
			System.out.println("DateUtils - Date2String");
			e.printStackTrace();
			return "";
		}
	}

	public static String DateForInsSQL(Date date) {
		return "'" + Date2String(date) + "'";
	}

	public static String DateForInsSQL235900(Date date) {
		return "'" + Date2String(date) + " 23:59:00'";
	}

	public static Date String2Date(String strDate) {
		return String2Date(strDate, "yyyy-MM-dd");
	}

	public static Date String2Date(String strDate, String Format) {
		if (strDate == null)
			return null;

		try {

			DateTimeFormat fmt = DateTimeFormat.getFormat(Format);

			if (strDate.length() > 10)
				strDate = strDate.substring(0, 10);

			return fmt.parse(strDate);
		} catch (Exception e) {
			System.out.println("DateUtils - String2Date");
			e.printStackTrace();
			return null;
		}
	}

	public static Date String2DateTime(String strDate) {
		return String2DateTime(strDate, "yyyy-MM-dd hh:mm:ss");
	}

	public static Date String2DateTime(String strDate, String Format) {
		if (strDate == null)
			return null;
		try {
			DateTimeFormat fmt = DateTimeFormat.getFormat(Format);
			strDate = strDate.substring(0, Format.length());
			return fmt.parse(strDate);
		} catch (Exception e) {
			System.out.println("DateUtils - String2DateTime");
			e.printStackTrace();
			return null;
		}
	}

	public static java.sql.Date String2SQLDate(String strDate) {

		if (strDate == null)
			return null;
		try {
			if (strDate.length() > 10)
				strDate = strDate.substring(0, 10);

			return java.sql.Date.valueOf(strDate);
		} catch (Exception e) {
			System.out.println("DateUtils - String2Date");
			e.printStackTrace();
			return null;
		}
	}

	public static java.sql.Timestamp String2SQLDateTime(String strDate) {

		// DebugUtils.D(strDate);
		if (strDate == null)
			return null;
		try {
			return java.sql.Timestamp.valueOf(strDate);
		} catch (Exception e) {
			System.out.println("DateUtils - String2DateTime");
			e.printStackTrace();
			return null;
		}
	}

	public static java.sql.Date Date2SQLDate(Date d) {

		return String2SQLDate(Date2String(d));
	}

	public static Timestamp Date2SQLDateTime(Date d) {

		// DebugUtils.D(String2SQLDateTime("2013-08-22 12:55:01"));

		return String2SQLDateTime(Date2String(d, "yyyy-MM-dd HH:mm:ss"));

	}

	public static Date FirstDayOfMonth(Date d) {
		Date newDate = d;

		newDate.setDate(1);

		return newDate;
	}

	public static Date LastDayOfMonth(Date d) {
		Date newDate;

		d.setDate(1);
		d.setMonth(d.getMonth() + 1);

		newDate = DateUtils.addDays(d, -1);

		return newDate;
	}

	public static Date FirstDayOfYear(Date d) {
		Date newDate = d;

		newDate.setDate(1);
		newDate.setMonth(0);

		return newDate;
	}

	public static Date LastDayOfYear(Date d) {
		Date newDate;

		d.setDate(1);
		d.setMonth(0);
		d.setYear(d.getYear() + 1);

		newDate = DateUtils.addDays(d, -1);

		return newDate;
	}

	public static String getYear() {
		return getYear(new Date());
	}

	public static String getYear(Date d) {

		String year = DateTimeFormat.getFormat("yyyy").format(d);
		return year;
	}

}
