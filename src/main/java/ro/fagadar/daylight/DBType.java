package ro.fagadar.daylight;

public class DBType {

	String columnName;
	String columnType;
	Object defaultValue;
	boolean autoincrement = false;
	int size1;
	int size2;

	public String toString() {
		return columnName + " " + columnType + " " + (defaultValue==null?"null":defaultValue.toString()) + " "
				+ (autoincrement ? "auto" : "not auto");
	}

}