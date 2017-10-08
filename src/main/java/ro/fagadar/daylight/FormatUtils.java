package ro.fagadar.daylight;

import com.google.gwt.i18n.client.NumberFormat;

public class FormatUtils {

	public static String DecimalFormat(int value) {
		NumberFormat fmt = NumberFormat.getDecimalFormat();
		return fmt.format(value);
	}

	public static String DecimalFormat(double value) {
		NumberFormat fmt = NumberFormat.getDecimalFormat();
		return fmt.format(value);
	}

	/* type conversions */
	public static Byte toByte(Object o) {
		return Byte.parseByte(o.toString());
	}

	public static Double toDouble(Object o) {
		return Double.parseDouble(o.toString());
	}

	public static Float toFloat(Object o) {
		return Float.parseFloat(o.toString());
	}

	public static int toInteger(Object o) {
		return Integer.parseInt(o.toString());
	}

	public static long toLong(Object o) {
		return Long.parseLong(o.toString());
	}

	public static short toShort(Object o) {
		return Short.parseShort(o.toString());
	}

	public static String toString(Object o) {
		return o.toString();
	}

	public static boolean isNumeric(String str) {
		if(str == null)
			return false;
		try {
			Double.parseDouble(str);
		} catch (NumberFormatException nfe) {
			return false;
		}
		return true;
	}

}
