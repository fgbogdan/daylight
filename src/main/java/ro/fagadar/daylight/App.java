package ro.fagadar.daylight;

import org.apache.log4j.Logger;

/**
 * test class
 *
 */

public class App {

	static Logger logger = Logger.getLogger(App.class);

	public static void main(String[] args) {
		/*
		 * log.trace("Trace Message!"); log.debug("Debug Message!");
		 * log.info("Info Message!"); log.warn("Warn Message!");
		 * log.error("Error Message!"); log.fatal("Fatal Message!");
		 */

		logger.info("Start App");

		
		DbManager.iniFileName = "daylight-test.ini";
		DbManager.getDB().initConnection();

		DBRecord R = DbManager.getDB().GetBlankDBRecord(null, "test", "", "", "ID");
		// R.put("ID", 1);
		R.put("NAME", "Virginia");
		DbManager.getDB().saveDBRecord(null, R);

		R = DbManager.getDB().GetBlankDBRecord(null, "test", "", "", "ID");
		// R.put("ID", 13);
		R.put("NAME", "Bogdan");
		DbManager.getDB().saveDBRecord(null, R);

		/* select */
		DBTable T = DbManager.getDB().getDBTable(null, "test", "ID", "ID=13");
		DebugUtils.D(T);
		if (T.reccount() == 1) {
			R = T.get(0);
			DebugUtils.D(R);
			R.put("NAME", "FloMar");
			DebugUtils.D(R);
			DbManager.getDB().saveDBTable(null, T);
		}
		// DbManager.getDB().deleteDBRecord(null, R);

		T = DbManager.getDB().getDBTable(null, "select * from test");
		DebugUtils.D(T);

	}
}
