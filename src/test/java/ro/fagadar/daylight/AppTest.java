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
		logger.info("Test suite ... run all from file");
		return new TestSuite(AppTest.class);
	}

	/**
	 * append blank test
	 */
	public void testApp() {

		DbManager.iniFileName = "daylight-test.ini";
		logger.info("init connection");
		DbManager.getDB().initConnection();

		String strSQLCommand;
		DBRecord ouser = null;

		strSQLCommand = "CREATE TABLE TEST (ID int, NUME char(10))";
		logger.info(strSQLCommand);
		DbManager.getDB().executeNoResultSet(ouser, strSQLCommand);

		strSQLCommand = "INSERT INTO TEST (ID,NUME) VALUES (1,'BOGDAN')";
		logger.info(strSQLCommand);
		DbManager.getDB().executeNoResultSet(ouser, strSQLCommand);

		strSQLCommand = "SELECT * FROM TEST";
		logger.info(strSQLCommand);
		DbManager.getDB().executeResultSetNoOutput(ouser, strSQLCommand);

		assertTrue(true);
	}

	/**
	 * Rigourous Test :-)
	 */
	public void testApp1() {
		System.out.println("test App1 ... when assert true - ok else false");
		assertTrue(true);
	}
}
