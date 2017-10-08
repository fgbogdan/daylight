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

		/* select */
		DBTable T = new DBTable();
		DbManager.getDB().getDBTable(null, T, "test", "ID", "ID=13");
		DebugUtils.D(T);
		DBRecord R = T.get(0);
		DebugUtils.D(R);
		R.put("NAME", "FlorinMarina");
		DebugUtils.D(R);
		DbManager.getDB().saveDBTable(null, T);

		//DbManager.getDB().deleteDBRecord(null, R);

		T = new DBTable();
		DbManager.getDB().getDBTable(null, T, "select * from test");
		DebugUtils.D(T);

	}
}