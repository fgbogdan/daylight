package ro.fagadar.daylight;

import org.apache.log4j.Logger;

public class DbManager {

	final static Logger logger = Logger.getLogger(DbManager.class);

	// static DB db = new DB();
	static DB db = null;

	public static String iniFileName = "nedefinit";

	public static DB getDB() {

		if (db == null) {
			db = new DB();
		}
		return db;
	}

}
