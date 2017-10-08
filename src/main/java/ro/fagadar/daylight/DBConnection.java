package ro.fagadar.daylight;

import java.sql.Connection;
import java.util.Date;

public class DBConnection {

	public Connection con = null;
	public String UserID = "";
	public Date bornDate = new Date();
	public boolean isinuse = false;
	public static String sqlServerName, sqlDatabase, sqlSufix, sqlIDFirma, FilesRepository, FilesRepository1, FilesRepository2;
	public static String strSQLUser, strSQLPassword;
	public static boolean isLog;
	// NSSQL, MYSQL, H2
	public static String dbType="";
	

	public boolean isDead() {
		if (5 < DateUtils.minDiff(new Date(), this.bornDate)) {
			return true;
		} else
			return false;
	}

	public void UseMe() {
		bornDate = new Date();
		isinuse = true;
		//System.out.println("Use : " + con.toString());
	}

	public void ReleaseMe() {
		bornDate = null;
		isinuse = false;
		//System.out.println("Release : " + con.toString());
	}

}
