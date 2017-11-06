package ro.fagadar.daylight;

import org.apache.log4j.Logger;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {

	final static Logger logger = Logger.getLogger(AppTest.class);

	/**
	 * Create the test case
	 *
	 * @param testName
	 *            name of the test case
	 */
	public AppTest(String testName) {
		super(testName);
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite() {
		return new TestSuite(AppTest.class);
	}

	/**
	 * append blank test
	 */
	public void testApp() {

		DbManager.iniFileName = "daylight-test.ini";
		DbManager.getDB().initConnection();

		String strSQLCommand;
		DBRecord ouser = null;

		strSQLCommand = "CREATE TABLE TEST (ID int, NAME char(10))";
		DbManager.getDB().executeNoResultSet(ouser, strSQLCommand);

		strSQLCommand = "INSERT INTO TEST (ID,NAME) VALUES (1,'BOGDAN')";
		DbManager.getDB().executeNoResultSet(ouser, strSQLCommand);

		strSQLCommand = "SELECT * FROM TEST";
		DBTable T = DbManager.getDB().getDBTable(ouser, strSQLCommand);

		logger.info(T.get(0).getString("NAME"));
		
		assertTrue(false);
	}

	/**
	 * Rigourous Test :-)
	 */
	public void testApp1() {
		assertTrue(true);
	}
}
