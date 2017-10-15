package ro.fagadar.daylight;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

public class DB {

	final static Logger logger = Logger.getLogger(DB.class);

	public ArrayList<DBConnection> connections = new ArrayList<DBConnection>();

	/**
	 * get a connection to the database from the connection pool or create a new one
	 * based on INI file
	 * 
	 * @param oUser
	 * @return
	 */
	public DBConnection getConn(DBRecord oUser) {

		String UserID = null;

		if (oUser != null)
			if (!oUser.tableName.isEmpty())
				UserID = oUser.getString("USERID");

		DBConnection con = null;

		String p_UserID;

		if (UserID == null)
			p_UserID = "";
		else
			p_UserID = UserID;

		for (int i = 0; i < connections.size(); i++) {
			con = (DBConnection) connections.get(i);

			/* only if not logged in or - a connection from the same user */
			if ("".equals(con.UserID) || p_UserID.equals(con.UserID)) {
				logger.info("Is My connection ...");
				if (con.isinuse) {
					/* do nothing */
				}
				if (!con.isinuse) {
					// i found a good one !
					con.UseMe();
					break;
				} else if (con.isDead()) {
					// if this connection is too old then remove it from the
					// list
					connections.remove(i);
					logger.info("Connection is dead ... remove ...");
					i--;
					con = null;
				} else {
					con = null;
				}
			} else
				con = null;
		}

		if (null == con) {
			// there are not enough connections !
			try {
				con = new DBConnection();
				con.con = getSQLConnection(oUser);
				con.UseMe();
				connections.add(con);
			} catch (Exception e) {
				con = null;
				logger.info("cannnot make a connection");
				logger.info(e.getMessage());
				e.printStackTrace();
			}
		}

		// another user
		if (!p_UserID.equals(con.UserID)) {
			// update userspid table with the ID of the current user
			con.UserID = p_UserID;
			logger.info("Connection used for new user ! " + con.UserID);
			Statement st;
			try {
				st = con.con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
				String strSQLCommand = "";
				switch (DBConnection.dbType) {
				case "MYSQL":
					strSQLCommand = "UPDATE userspid SET UserID='" + p_UserID + "' WHERE spid=connection_id();";
					break;
				case "MSSQL":
					strSQLCommand = "UPDATE userspid SET UserID='" + p_UserID + "' WHERE spid=@@spid";
					break;
				case "H2":
					strSQLCommand = ";";
					break;
				}
				st.execute(strSQLCommand);
				logger.info("SPID - set !");
			} catch (SQLException e1) {
				logger.info("UPDATE userspid");
				logger.info(e1.getMessage());
				e1.printStackTrace();
			}
		}

		return con;

	}

	/**
	 * read from INI all the connection properties (and more)
	 */
	public void initConnection() {

		String strServerName = "", strSQLUser = "", strSQLPassword = "", strSQLDatabase = "";
		String strSQLSufix = "", strSQLFirma = "", strFilesRepository = "";
		String strSQLType, strisLog = "NO";

		try {

			Properties pro = new Properties();

			logger.info("the ini file is:" + System.getProperty("user.dir") + "\\" + DbManager.iniFileName);

			pro.load(new FileInputStream(DbManager.iniFileName));

			// sql server
			strServerName = pro.getProperty("SQLSERVER");
			if (strServerName == null)
				strServerName = "<undefined>";
			strServerName = strServerName.trim();
			DBConnection.sqlServerName = strServerName;
			String cumulativeMessage = "";

			cumulativeMessage += " / SQLSERVER=" + strServerName;

			// sql user
			strSQLUser = pro.getProperty("SQLUSER");
			if (strSQLUser == null)
				strSQLUser = "<undefined>";
			strSQLUser = strSQLUser.trim();
			DBConnection.strSQLUser = strSQLUser;
			cumulativeMessage += " / SQLUSER=" + strSQLUser;

			// sql password
			strSQLPassword = pro.getProperty("SQLPASSWORD");
			if (strSQLPassword == null)
				strSQLPassword = "<undefined>";
			strSQLPassword = strSQLPassword.trim();
			DBConnection.strSQLPassword = strSQLPassword;
			cumulativeMessage += " / SQLPASSWORD=" + strSQLPassword;

			// database
			strSQLDatabase = pro.getProperty("SQLDATABASE");
			if (strSQLDatabase == null)
				strSQLDatabase = "<undefined>";
			strSQLDatabase = strSQLDatabase.trim();
			DBConnection.sqlDatabase = strSQLDatabase;
			cumulativeMessage += " / SQLDATABASE=" + strSQLDatabase;

			// database sufix
			strSQLSufix = pro.getProperty("SQLSUFIX");
			if (strSQLSufix == null)
				strSQLSufix = "<undefined>";
			strSQLSufix = strSQLSufix.trim();
			DBConnection.sqlSufix = strSQLSufix;
			cumulativeMessage += " / SQLSUFIX=" + strSQLSufix;

			// firm identification
			strSQLFirma = pro.getProperty("SQLFIRMA");
			if (strSQLFirma == null)
				strSQLFirma = "<undefined>";
			strSQLFirma = strSQLFirma.trim();
			if (strSQLFirma.isEmpty())
				strSQLFirma = "FRM";
			DBConnection.sqlIDFirma = strSQLFirma;
			cumulativeMessage += " / SQLFIRMA=" + strSQLFirma;

			// database type
			strSQLType = pro.getProperty("SQLTYPE");
			if (strSQLType == null)
				strSQLType = "";
			strSQLType = strSQLType.trim();
			if (strSQLType.isEmpty())
				strSQLType = "MSSQL";
			if (!strSQLType.equals("MSSQL") && !strSQLType.equals("MYSQL") && !strSQLType.equals("H2"))
				throw new Exception("SQLTYPE not defined correctly in ini file (values accepted are MSSQL or MYSQL)");
			DBConnection.dbType = strSQLType;
			cumulativeMessage += " / SQLTYPE=" + strSQLType;

			// logging
			strisLog = pro.getProperty("ISLOG");
			if (strisLog == null)
				strisLog = "<undefined>";
			strisLog = strisLog.trim();
			if (strisLog.isEmpty())
				strisLog = "NO";
			DBConnection.isLog = strisLog.equals("YES") ? true : false;
			cumulativeMessage += " / ISLOG=" + strisLog;

			// Files Repository
			strFilesRepository = pro.getProperty("FILES_REPOSITORY");
			if (strFilesRepository == null)
				strFilesRepository = "<undefined>";
			strFilesRepository = strFilesRepository.trim();
			DBConnection.FilesRepository = strFilesRepository;
			cumulativeMessage += " / FILES_REPOSITORY=" + strFilesRepository;

			// Files Repository1
			strFilesRepository = pro.getProperty("FILES_REPOSITORY1");
			if (strFilesRepository == null)
				strFilesRepository = "<undefined>";
			strFilesRepository = strFilesRepository.trim();
			DBConnection.FilesRepository1 = strFilesRepository;
			cumulativeMessage += " / FILES_REPOSITORY1=" + strFilesRepository;

			// Files Repository2
			strFilesRepository = pro.getProperty("FILES_REPOSITORY2");
			if (strFilesRepository == null)
				strFilesRepository = "<undefined>";
			strFilesRepository = strFilesRepository.trim();
			DBConnection.FilesRepository2 = strFilesRepository;
			cumulativeMessage += " / FILES_REPOSITORY2=" + strFilesRepository;

			logger.info(cumulativeMessage);

		} catch (Exception ex) {
			logger.error(ex.getMessage());
			logger.error("---");
			logger.error("the ini file must be in:" + System.getProperty("user.dir"));
		}

	}

	/**
	 * get A new SQL connection for the current user
	 * 
	 * @param oUser
	 * @return
	 */
	public Connection getSQLConnection(DBRecord oUser) {

		logger.info("Create a new connection");

		Connection conn = null;

		if (DBConnection.dbType.isEmpty())
			initConnection();

		// set the connection class
		try {
			switch (DBConnection.dbType) {
			case "MYSQL":
				Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
				break;
			case "MSSQL":
				Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance();
				break;
			case "H2":
				break;
			}

		} catch (Exception e) {
			logger.info("getConn ... ClassForName");
			logger.info(e.getMessage());
			e.printStackTrace();
		}

		// String url =
		// "jdbc:sqlserver://SrvVM2\\EX1;user=sa;password=bcmanager;databaseName=dbUnitM07_FRM;";
		// url =
		// "jdbc:mysql://xx.xx.xx.xx:3306;user=xxx;password=xxx;databaseName=xxx;";
		String url = "";

		switch (DBConnection.dbType) {
		case "MYSQL":
			url = "jdbc:mysql://" + DBConnection.sqlServerName + "/" + DBConnection.sqlDatabase
					+ "?zeroDateTimeBehavior=convertToNull&autoReconnect=true&relaxAutoCommit=true&allowMultiQueries=true";
			break;
		case "MSSQL":
			url = "jdbc:sqlserver://" + DBConnection.sqlServerName + ";user=" + DBConnection.strSQLUser + ";password="
					+ DBConnection.strSQLPassword + ";databaseName=" + DBConnection.sqlDatabase + ";";

			break;
		case "H2":
			url = "jdbc:h2:~/" + DBConnection.sqlDatabase + ", " + DBConnection.strSQLUser + ", "
					+ DBConnection.strSQLPassword;
			break;
		}
		logger.info("Connect to server ... " + url);

		String sDataBase = "SqlDatabase=" + DBConnection.sqlDatabase;
		logger.info(sDataBase);

		try {

			switch (DBConnection.dbType) {
			case "MYSQL":
				conn = DriverManager.getConnection(url, DBConnection.strSQLUser, DBConnection.strSQLPassword);
				break;
			case "MSSQL":
				conn = DriverManager.getConnection(url);

				break;
			case "H2":
				conn = DriverManager.getConnection("jdbc:h2:~/test", "sa", "");
				break;
			}

			/*
			 * if the connection is succesfull and TheApp.loginInfo.R is not null - set the
			 * information in UserSpid
			 */
			SetUserSPID(oUser, conn);

		} catch (SQLException e) {
			logger.info("getConn ... get connection(url)");
			logger.info(e.getMessage());
			e.printStackTrace();
		}

		return conn;
	}

	/**
	 * return a record from a specified table based on a specified pair column/value
	 * 
	 * @param oUser
	 * @param oRecord
	 * @param tableName
	 * @param colName
	 * @param colValue
	 */
	public DBRecord GetDBRecord(DBRecord oUser, String tableName, String colName, String colValue) {

		DBRecord oRecord = new DBRecord();

		DBConnection con = this.getConn(oUser);
		try {

			Connection conn = con.con;

			String strSQLCommand = "";

			switch (DBConnection.dbType) {
			case "MYSQL":
				strSQLCommand = "SELECT * FROM " + tableName + " WHERE " + colName + "= ? LIMIT 1;";
				break;
			case "MSSQL":
				strSQLCommand = "SELECT TOP 1 * FROM " + tableName + " WITH (NOLOCK) WHERE " + colName + "= ? ";
				break;
			case "H2":
				strSQLCommand = "SELECT TOP 1 * FROM " + tableName + " WHERE " + colName + "= ? ";
				break;
			}

			PreparedStatement pst = conn.prepareStatement(strSQLCommand);

			ResultSet rs = null;
			try {

				pst.setString(1, colValue);

				rs = pst.executeQuery();

				logger.info(rs);

			} catch (Exception e) {
				logger.info(e.toString());
			}

			if (rs.next()) {

				ResultSetMetaData rsmdResult = null;

				int intNoCols = 0;
				int intColumnType = 0;
				String strColname = null;

				try {
					rsmdResult = rs.getMetaData();
					intNoCols = rsmdResult.getColumnCount();

					oRecord.tableName = tableName;
					oRecord.KeyName = colName;
					oRecord.KeyValue = colValue;
					oRecord.isNew = false;

					DBType type = new DBType();
					for (int intCount = 1; intCount <= intNoCols; intCount++) {
						// NOTE: THE COLUMN NAMES WILL ALWAYS BE STORED IN
						// UPPERCASE, HENCE NEED TO BE RETRIEVED IN UPPER CASE
						strColname = rsmdResult.getColumnName(intCount).toUpperCase();
						intColumnType = rsmdResult.getColumnType(intCount);
						type = InterpretType(intColumnType);
						type.columnName = strColname;
						type.autoincrement = rsmdResult.isAutoIncrement(intCount);
						oRecord.dbStructure.add(type);
						oRecord.put_original(strColname, getFromRS(rs, type));
					}
				} catch (Exception e) {
					logger.info(strColname);
					logger.info(intColumnType);
					logger.info(e.getMessage());
					e.printStackTrace();
				}

			}

		} catch (SQLException e) {
			logger.info("GetDBRecord ... get connection");
			logger.info(e.getMessage());
			e.printStackTrace();

		}

		con.ReleaseMe();

		oRecord.isChanged = false;

		return oRecord;
	}

	/**
	 * return a record from a specified sql command
	 * 
	 * @param oUser
	 * @param oRecord
	 * @param strSQLCommand
	 */
	public DBRecord GetDBRecordForConditon(DBRecord oUser, String strSQLCommand) {
		return GetDBRecordForConditon(oUser, "", strSQLCommand);
	}

	/**
	 * return a record from a specified table based on a specified sql condition
	 * 
	 * @param oUser
	 * @param oRecord
	 * @param tableName
	 * @param strSQLCond
	 */
	public DBRecord GetDBRecordForConditon(DBRecord oUser, String tableName, String strSQLCond) {

		DBRecord oRecord = new DBRecord();

		DBConnection con = this.getConn(oUser);

		try {

			Connection conn = con.con;

			Statement st = conn.createStatement();

			ResultSet rs = null;
			try {
				String strSQLCommand = "";
				if (tableName.isEmpty())
					strSQLCommand = strSQLCond;
				else {

					switch (DBConnection.dbType) {
					case "MYSQL":
						strSQLCommand = "SELECT * FROM " + tableName + " WHERE " + strSQLCond + " LIMIT 1;";
						break;
					case "MSSQL":
						strSQLCommand = "SELECT TOP 1 * FROM " + tableName + " WITH (NOLOCK) WHERE " + strSQLCond;
						break;
					case "H2":
						strSQLCommand = "SELECT TOP 1 * FROM " + tableName + " WITH (NOLOCK) WHERE " + strSQLCond;
						break;
					}

				}

				rs = st.executeQuery(strSQLCommand);
			} catch (Exception e) {
				logger.info("GetRecordForConditon");
				logger.info(e.toString());
			}

			// first record
			if (rs.next()) {

				ResultSetMetaData rsmdResult = null;

				int intNoCols = 0;
				String strColname = null;
				int intColumnType = 0;

				try {
					rsmdResult = rs.getMetaData();
					intNoCols = rsmdResult.getColumnCount();

					oRecord.tableName = tableName;
					oRecord.KeyName = "";
					oRecord.KeyValue = "";

					try {

						DBType type = new DBType();
						for (int intCount = 1; intCount <= intNoCols; intCount++) {
							// NOTE: THE COLUMN NAMES WILL ALWAYS BE STORED IN
							// UPPERCASE, HENCE NEED TO BE RETRIEVED IN UPPER CASE
							strColname = rsmdResult.getColumnName(intCount).toUpperCase();
							intColumnType = rsmdResult.getColumnType(intCount);
							type = InterpretType(intColumnType);
							type.columnName = strColname;
							type.autoincrement = rsmdResult.isAutoIncrement(intCount);
							oRecord.dbStructure.add(type);
							oRecord.put_original(strColname, getFromRS(rs, type));
						}

					} catch (Exception e) {
						logger.info(strColname);
						logger.info(intColumnType);
						logger.info(e.getMessage());
						e.printStackTrace();
					}
				} catch (SQLException e) {
					logger.info("put_original");
					logger.info(e.getMessage());
					e.printStackTrace();
				}
			}

		} catch (SQLException e) {
			logger.info("GetDBRecordForCondition ... get connection");
			logger.info(e.getMessage());
			e.printStackTrace();

		}

		con.ReleaseMe();
		oRecord.isChanged = false;

		return oRecord;
	}

	/**
	 * return a record from a specified table based on a specified list of pairs
	 * column/value
	 * 
	 * @param conn
	 * @param oRecord
	 * @param tableName
	 * @param colNameArr
	 * @param colValueArr
	 */
	public DBRecord GetDBRecordwithConn(Connection conn, String tableName, ArrayList<String> colNameArr,
			ArrayList<String> colValueArr) {

		DBRecord oRecord = new DBRecord();

		if (colNameArr.size() > 0 && colValueArr.size() > 0 && colNameArr.size() == colValueArr.size()) {
			try {

				String strSQLCommand = "";
				String sqlCond = "1 = 1 ";

				for (int i = 0; i < colNameArr.size(); i++) {
					sqlCond += " AND " + colNameArr.get(i) + " = ?";
				}

				switch (DBConnection.dbType) {
				case "MYSQL":
					strSQLCommand = "SELECT * FROM " + tableName + " WHERE " + sqlCond + " LIMIT 1;";
					break;
				case "MSSQL":
					strSQLCommand = "SELECT TOP 1 * FROM " + tableName + " WITH (NOLOCK) WHERE " + sqlCond;
					break;
				case "H2":
					strSQLCommand = "SELECT TOP 1 * FROM " + tableName + " WITH (NOLOCK) WHERE " + sqlCond;
					break;
				}

				ResultSet rs = null;

				try {

					PreparedStatement pst = conn.prepareStatement(strSQLCommand);
					for (int i = 0; i < colNameArr.size(); i++) {
						pst.setString(i + 1, colValueArr.get(i));
					}
					rs = pst.executeQuery();

				} catch (Exception e) {
					logger.info(e.toString());
				}

				if (rs.next()) {

					ResultSetMetaData rsmdResult = null;

					int intNoCols = 0;
					int intColumnType = 0;
					String strColname = null;

					try {
						rsmdResult = rs.getMetaData();
						intNoCols = rsmdResult.getColumnCount();

						oRecord.tableName = tableName;
						oRecord.KeyName = colNameArr.get(0);
						oRecord.KeyValue = colValueArr.get(0);

						try {

							DBType type = new DBType();
							for (int intCount = 1; intCount <= intNoCols; intCount++) {
								// NOTE: THE COLUMN NAMES WILL ALWAYS BE STORED IN
								// UPPERCASE, HENCE NEED TO BE RETRIEVED IN UPPER CASE
								strColname = rsmdResult.getColumnName(intCount).toUpperCase();
								intColumnType = rsmdResult.getColumnType(intCount);
								type = InterpretType(intColumnType);
								type.columnName = strColname;
								type.autoincrement = rsmdResult.isAutoIncrement(intCount);
								oRecord.dbStructure.add(type);
								oRecord.put_original(strColname, getFromRS(rs, type));
							}

						} catch (Exception e) {
							logger.info(strColname);
							logger.info(intColumnType);
							logger.info(e.getMessage());
							e.printStackTrace();
						}
					} catch (Exception e) {
						logger.info(e.getMessage());
						e.printStackTrace();
					}

				}

			} catch (SQLException e) {
				logger.info("get connection");
				logger.info(e.getMessage());
				e.printStackTrace();

			}
		}

		oRecord.isChanged = false;

		return oRecord;

	} // GetDBRecordwithConn

	/**
	 * save the current R record in the database
	 * 
	 * @param oUser
	 * @param oRecord
	 * @return
	 */
	public String saveDBRecord(DBRecord oUser, DBRecord oRecord) {

		// return string
		String strErrorMessage = "";
		String strGeneratedKey = "";
		DBConnection con = this.getConn(oUser);

		Connection conn = con.con;
		String strInsert = "";
		if (oRecord.isNew) {
			// generate the insert string
			strInsert += "INSERT INTO ";
			strInsert += oRecord.tableName;
			strInsert += " ( ";
			strInsert += oRecord.dbStructure.getFieldsForInsert();
			strInsert += " ) ";
			strInsert += " VALUES ";
			strInsert += " ( ";
			strInsert += oRecord.dbStructure.getQuestionMarksForSet();
			strInsert += " ) ";
		} else {
			// generate the update
			strInsert += "UPDATE ";
			strInsert += oRecord.tableName;
			strInsert += " SET ";
			strInsert += oRecord.dbStructure.getFieldsForSet();
			strInsert += " WHERE ";
			strInsert += oRecord.KeyName;
			strInsert += "=?";

		}

		logger.info(strInsert);

		PreparedStatement ps;
		try {
			ps = conn.prepareStatement(strInsert, PreparedStatement.RETURN_GENERATED_KEYS);

			oRecord.setPreparedStatementValues(ps);

			ps.executeUpdate();

			ResultSet generatedKeys = ps.getGeneratedKeys();
			if (generatedKeys.next()) {
				strGeneratedKey = generatedKeys.getString(1);
			}

			// if we log changes
			if (oRecord.isLog) {
				oRecord.put(oRecord.KeyValue, strErrorMessage);
				WriteLog(oUser, oRecord, oRecord.KeyValue, "", strErrorMessage);
			}

			// commit the transaction
			if (!conn.getAutoCommit())
				conn.commit();

			// close object
			ps.close();
			//
			con.ReleaseMe();

			if (strErrorMessage.isEmpty()) {
				oRecord.isChanged = false;
				strErrorMessage = strGeneratedKey;
			}

		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
		return strErrorMessage;
	}

	/**
	 * delete a specified record from a specified table based on a specified column
	 * and value
	 * 
	 * @param oUser
	 * @param tableName
	 * @param colName
	 * @param colValue
	 * @return
	 */
	public String deleteDBRecord(DBRecord oUser, String tableName, String colName, String colValue) {

		DBConnection con = this.getConn(oUser);
		String strErrorMessage = "";

		try {

			Connection conn = con.con;

			PreparedStatement ps = conn.prepareStatement("DELETE FROM " + tableName + " WHERE colName  = ?");
			ps.setString(1, colValue);
			ps.executeUpdate();

			// commit the transaction
			if (!conn.getAutoCommit())
				conn.commit();

			// close object
			ps.close();

		} catch (SQLException e) {
			logger.info("DeleteDBRecord ... get connection - SQLException");
			logger.info(e.getMessage());
			e.printStackTrace();
			strErrorMessage = e.toString();
		} catch (Exception e) {
			logger.error("DeleteDBRecord ... get connection - Exception");
			logger.error(e.getMessage());
			e.printStackTrace();
			strErrorMessage = e.toString();
		}

		con.ReleaseMe();

		return strErrorMessage;
	}

	/**
	 * delete a soecified DBRecord
	 * 
	 * @param oUser
	 * @param oRecord
	 */
	public void deleteDBRecord(DBRecord oUser, DBRecord oRecord) {
		try {
			deleteDBRecord(oUser, oRecord.tableName, oRecord.KeyName, oRecord.KeyValue);
		} catch (Exception e) {
			logger.error("DeleteDBRecord DBRecord ... get connection");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * delete using a sql command
	 * 
	 * @param oUser
	 * @param strSQLCommand
	 * @return
	 */
	public String deleteDBRecord(DBRecord oUser, String strSQLCommand) {

		DBConnection con = this.getConn(oUser);
		String strErrorMessage = "";

		try {

			Connection conn = con.con;

			Statement st = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

			ResultSet rs = null;
			try {

				rs = st.executeQuery(strSQLCommand);
			} catch (Exception e) {
				logger.info(e.toString());
			}

			if (rs.next()) {
				// delete
				rs.deleteRow();
				conn.commit();
			}
		} catch (SQLException e) {
			logger.info("DeleteDBRecord ... get connection");
			logger.info(e.getMessage());
			e.printStackTrace();
			strErrorMessage = e.toString();
		}

		con.ReleaseMe();

		return strErrorMessage;
	}

	/**
	 * get a 2 columns list from a table
	 * 
	 * @param oUser
	 * @param oList
	 * @param p_strTableName
	 * @param p_strShowField
	 * @param p_strKeyField
	 * @param p_strFilterCondition
	 */
	public void GetList2D(DBRecord oUser, List2D oList, String p_strTableName, String p_strShowField,
			String p_strKeyField, String p_strFilterCondition) {
		GetList2D(oUser, oList, p_strTableName, p_strShowField, p_strKeyField, p_strFilterCondition, "");
	}

	/**
	 * get a 2 columns list from a table with order params
	 * 
	 * @param oUser
	 * @param oList
	 * @param p_strTableName
	 * @param p_strShowField
	 * @param p_strKeyField
	 * @param p_strFilterCondition
	 * @param p_strOrder
	 */
	public void GetList2D(DBRecord oUser, List2D oList, String p_strTableName, String p_strShowField,
			String p_strKeyField, String p_strFilterCondition, String p_strOrder) {

		String strSQLCommand = "";

		switch (DBConnection.dbType) {
		case "MYSQL":
			strSQLCommand = " SELECT " + p_strShowField + " as ShowFld, " + p_strKeyField + " as KeyFld FROM "
					+ p_strTableName + " ";
			break;
		case "MSSQL":
			strSQLCommand = " SELECT " + p_strShowField + " as ShowFld, " + p_strKeyField + " as KeyFld FROM "
					+ p_strTableName + " WITH (NOLOCK) ";
			break;
		case "H2":
			strSQLCommand = " SELECT " + p_strShowField + " as ShowFld, " + p_strKeyField + " as KeyFld FROM "
					+ p_strTableName + " WITH (NOLOCK) ";
			break;
		}

		if (!p_strFilterCondition.isEmpty()) {
			strSQLCommand = strSQLCommand + " WHERE " + p_strFilterCondition;
		}
		if (p_strOrder.equals(""))
			p_strOrder = p_strShowField;
		strSQLCommand = strSQLCommand + " ORDER BY " + p_strOrder;

		GetList2D(oUser, oList, strSQLCommand, "");

	}

	/**
	 * get a 2 columns list from a table using an sql commad
	 * 
	 * @param oUser
	 * @param oList
	 * @param strSQLCommand
	 * @param strFilterCondition
	 */
	public void GetList2D(DBRecord oUser, List2D oList, String strSQLCommand, String strFilterCondition) {

		DBConnection con = this.getConn(oUser);

		try {

			Connection conn = con.con;

			Statement st = conn.createStatement();

			ResultSet rs = null;
			try {
				if (!strFilterCondition.isEmpty()) {
					if (strSQLCommand.toUpperCase().contains(" WHERE "))
						strSQLCommand = strSQLCommand + " AND " + strFilterCondition;
					else
						strSQLCommand = strSQLCommand + " WHERE " + strFilterCondition;
				}

				rs = st.executeQuery(strSQLCommand);

			} catch (Exception e) {
				logger.info(e.toString());
			}

			if (rs != null)
				try {

					while (rs.next()) {
						String2D stringXD = null;
						if (strSQLCommand.indexOf("X1Fld") > 0 && strSQLCommand.indexOf("X1DFld") > 0)
							stringXD = new String2D(rs.getString("ShowFld"), rs.getString("KeyFld"),
									rs.getString("X1Fld"), rs.getDouble("X1DFld"));
						else {
							if (strSQLCommand.indexOf("X1Fld") > 0)
								stringXD = new String2D(rs.getString("ShowFld"), rs.getString("KeyFld"),
										rs.getString("X1Fld"), 0d);
							else
								stringXD = new String2D(rs.getString("ShowFld"), rs.getString("KeyFld"));
						}
						oList.add(stringXD);
					}
					rs.close();
				} catch (SQLException e) {
					logger.error("Get ListXD");
					logger.error(e.getMessage());
					e.printStackTrace();
				}

		} catch (SQLException e) {
			logger.error("GetList2D ... get connection");
			logger.error(e.getMessage());
			e.printStackTrace();

		}

		con.ReleaseMe();
	}

	@SuppressWarnings("unused")
	private static void checkResultSet(ResultSet RS) {
		try {
			int concurrency = RS.getConcurrency();

			if (concurrency == ResultSet.CONCUR_UPDATABLE)
				logger.info("ResultSet is Updateable");
			else
				logger.info("ResultSet is NOT Updateable");
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			System.exit(0);
		}
	}

	/**
	 * return a entire resultset using an sql command
	 * 
	 * @param oUser
	 * @param oTable
	 * @param p_strSQLCommand
	 * @return
	 */
	public DBTable getDBTable(DBRecord oUser, String p_strSQLCommand) {
		return getDBTable(oUser, "", "", p_strSQLCommand);
	}

	/**
	 * return a entire resultset using a specified pair key/value
	 * 
	 * @param oUser
	 * @param oTable
	 * @param p_strSQLCommand
	 * @return
	 */

	public DBTable getDBTable(DBRecord oUser, String p_strTableName, String p_strKeyName, String p_strFilterCondition) {
		return getDBTable(oUser, p_strTableName, p_strKeyName, p_strFilterCondition, "");
	}

	/**
	 * return a resultset using a filter and a order condition
	 * 
	 * @param oUser
	 * @param oTable
	 * @param p_strTableName
	 * @param p_strKeyName
	 * @param p_strFilterCondition
	 * @param p_strOrderCondition
	 * @return
	 */
	public DBTable getDBTable(DBRecord oUser, String p_strTableName, String p_strKeyName, String p_strFilterCondition,
			String p_strOrderCondition) {

		DBTable oTable = new DBTable();

		DBConnection con = this.getConn(oUser);

		oTable = getDBTableWithConn(con.con, p_strTableName, p_strKeyName, p_strFilterCondition, p_strOrderCondition);
		con.ReleaseMe();

		return oTable;
	}

	public DBTable getDBTableWithConn(Connection conn, String p_strTableName, String p_strKeyName,
			String p_strFilterCondition, String p_strOrderCondition) {

		DBTable oTable = new DBTable();

		// string de return
		String strErrorMessage = "";

		// empty the table
		oTable.clear();

		try {

			Statement st = conn.createStatement();

			ResultSet rs = null;
			// if p_strTableName is empty ... use the p_strFilterCondition for
			// call
			String strSQLCommand = "";
			if (p_strTableName.isEmpty()) {
				strSQLCommand = p_strFilterCondition;

			} else {

				switch (DBConnection.dbType) {
				case "MYSQL":
					strSQLCommand = " SELECT * FROM " + p_strTableName + " ";
					break;
				case "MSSQL":
					strSQLCommand = " SELECT * FROM " + p_strTableName + " WITH (NOLOCK) ";
					break;
				case "H2":
					strSQLCommand = " SELECT * FROM " + p_strTableName;
					break;
				}

				if (!p_strFilterCondition.isEmpty()) {
					strSQLCommand = strSQLCommand + " WHERE " + p_strFilterCondition;
				}

				if (!p_strOrderCondition.isEmpty()) {
					strSQLCommand = strSQLCommand + " ORDER BY " + p_strOrderCondition;
				}

			}

			// call
			try {
				rs = st.executeQuery(strSQLCommand);
				// logger.info(strSQLCommand);
			} catch (Exception e) {
				logger.info("GetDBTable ... executeQuery");
				logger.info(strSQLCommand);
				logger.info(e.toString());
				strErrorMessage = e.toString();

			}

			// process result

			oTable.tableName = p_strTableName;
			oTable.KeyName = p_strKeyName;

			boolean bFirstTime = true;
			int nrecno = 0;
			if (rs != null) {
				while (rs.next()) {
					// generate each record and put it in the list
					ResultSetMetaData rsmdResult = null;

					int intNoCols = 0;
					String strColname = null;
					int intColumnType = 0;
					DBRecord oDBRecord = new DBRecord();
					nrecno++;
					oDBRecord.recno = nrecno;
					try {
						rsmdResult = rs.getMetaData();
						intNoCols = rsmdResult.getColumnCount();

						oDBRecord.tableName = p_strTableName;
						oDBRecord.KeyName = p_strKeyName;
						oDBRecord.KeyValue = "";

						try {

							DBType type = new DBType();
							for (int intCount = 1; intCount <= intNoCols; intCount++) {
								// NOTE: THE COLUMN NAMES WILL ALWAYS BE STORED IN
								// UPPERCASE, HENCE NEED TO BE RETRIEVED IN UPPER CASE
								strColname = rsmdResult.getColumnName(intCount).toUpperCase();
								intColumnType = rsmdResult.getColumnType(intCount);
								try {
									type = InterpretType(intColumnType);
								} catch (Exception e) {
									logger.info("InterpretType type not found");
									logger.info("Column Name: " + strColname);
									logger.info("Numeric value of type: " + intColumnType);
								}
								type.columnName = strColname;
								type.autoincrement = rsmdResult.isAutoIncrement(intCount);
								oDBRecord.dbStructure.add(type);
								oDBRecord.put_original(strColname, getFromRS(rs, type));

								// save the fields names
								if (bFirstTime) {
									oTable.Fields.add(strColname);
									oTable.FieldTypes.add(getFieldType(rsmdResult.getColumnType(intCount)));
								}

								// NOTE: THE COLUMN NAMES WILL ALWAYS BE STORED
								// IN
								// UPPERCASE, HENCE NEED TO BE RETRIEVED IN
								// UPPER
								// CASE
							}
						} catch (Exception e) {
							logger.info("GetDBTable fill DBRecord... for ");
							logger.info(e.getMessage());
							e.printStackTrace();
							strErrorMessage = e.toString();
						}

						bFirstTime = false;

						// key value if exists
						if (!oDBRecord.KeyName.isEmpty())
							oDBRecord.KeyValue = oDBRecord.getString(oDBRecord.KeyName);

						// add in Table
						oTable.add(oDBRecord);

					} catch (SQLException e) {
						logger.info("GetDBTable fill DBRecord... body");
						logger.info(e.getMessage());
						e.printStackTrace();
						strErrorMessage = e.toString();
					}

				} // while
				rs.close();
			} // if != null

		} catch (SQLException e) {
			logger.info("GetDBTable ... get connection");
			logger.info(e.getMessage());
			e.printStackTrace();
			strErrorMessage = e.toString();

		}

		oTable.strErrorMessage = strErrorMessage;
		return oTable;
	}

	/**
	 * save the modified/deleted/inserted records from a table into the database
	 * 
	 * @param oUser
	 * @param oTable
	 * @return
	 */
	public DBTable saveDBTable(DBRecord oUser, DBTable oTable) {
		String strErrorMessage = "";
		// if the Table is not updatable ...
		if (oTable.tableName.isEmpty())
			return oTable;

		// for each record in DBTable - update, delete or insert into the
		// database
		// update and insert
		for (int i = 0; i < oTable.Records.size(); i++) {
			strErrorMessage += DB.this.saveDBRecord(oUser, oTable.get(i));
			oTable.get(i).isNew = false;
		}
		// delete
		for (int i = 0; i < oTable.DeletedRecords.size(); i++) {
			DB.this.deleteDBRecord(oUser, oTable.DeletedRecords.get(i));
		}
		if (!strErrorMessage.isEmpty())
			logger.info(strErrorMessage);
		return oTable;

	}

	/**
	 * execute an sql without returning an resultset
	 * 
	 * @param oUser
	 * @param strSQLCommand
	 * @return
	 */

	public String executeNoResultSet(DBRecord oUser, String strSQLCommand) {
		String strErrorMessage = "";

		DBConnection con = this.getConn(oUser);
		strErrorMessage = executeNoResultSetWithConn(con.con, strSQLCommand);
		con.ReleaseMe();
		return strErrorMessage;
	}

	/**
	 * execute an sql over a specified connection
	 * 
	 * @param conn
	 * @param strSQLCommand
	 * @return
	 */
	public String executeNoResultSetWithConn(Connection conn, String strSQLCommand) {

		String strErrorMessage = "";
		try {

			Statement st1 = conn.createStatement();

			try {

				st1.executeUpdate(strSQLCommand);
				st1.close();
			} catch (Exception e) {
				logger.info(e.toString());
				strErrorMessage = e.toString();
			}

		} catch (SQLException e) {
			logger.info("executeNoResultSet ... get connection");
			logger.info(e.getMessage());
			e.printStackTrace();
			strErrorMessage = e.toString();
			strErrorMessage = e.toString();
		}

		return strErrorMessage;
	}

	/**
	 * execute an sql command without output
	 * 
	 * @param oUser
	 * @param strSQLCommand
	 * @return
	 */
	public String executeResultSetNoOutput(DBRecord oUser, String strSQLCommand) {
		DBConnection con = this.getConn(oUser);
		String strErrorMessage = "";
		strErrorMessage = executeResultSetNoOutputWithConn(con.con, strSQLCommand);
		con.ReleaseMe();
		return strErrorMessage;
	}

	/**
	 * execute an sql over a specified connection
	 * 
	 * @param conn
	 * @param strSQLCommand
	 * @return
	 */
	public String executeResultSetNoOutputWithConn(Connection conn, String strSQLCommand) {

		String strErrorMessage = "";

		try {

			Statement st1 = conn.createStatement();

			try {

				st1.executeQuery(strSQLCommand);
				st1.close();
			} catch (Exception e) {
				logger.info(e.toString());
				strErrorMessage = e.toString();
			}

		} catch (SQLException e) {
			logger.info("executeNoResultSet ... get connection");
			logger.info(e.getMessage());
			e.printStackTrace();
			strErrorMessage = e.toString();
		}

		return strErrorMessage;
	}

	/**
	 * write in the log table the changes
	 * 
	 * @param oRecord
	 * @param strColumnName
	 * @param strOldValue
	 * @param strNewValue
	 */
	void WriteLog(DBRecord oUser, DBRecord oRecord, String strColumnName, String strOldValue, String strNewValue) {

		DBConnection con = this.getConn(oUser);

		Connection conn = con.con;

		String strTableName = oRecord.tableName;
		String strKeyValue = oRecord.KeyValue;

		/*
		 * MYSQL CREATE TABLE `log_audit` ( `table_name` varchar(30) DEFAULT NULL,
		 * `key_value` varchar(45) DEFAULT NULL, `column_name` varchar(45) DEFAULT NULL,
		 * `old_value` varchar(100) DEFAULT NULL, `new_value` varchar(100) DEFAULT NULL,
		 * `alias` varchar(45) DEFAULT NULL, `dtmDateStamp` datetime DEFAULT NULL )
		 * ENGINE=InnoDB DEFAULT CHARSET=utf8$$
		 * 
		 * 
		 * MSSQL CREATE TABLE [dbo].[log_audit]( [table_name] [varchar](30) NULL DEFAULT
		 * (NULL), [key_value] [varchar](45) NULL DEFAULT (NULL), [column_name]
		 * [varchar](45) NULL DEFAULT (NULL), [old_value] [varchar](100) NULL DEFAULT
		 * (NULL), [new_value] [varchar](100) NULL DEFAULT (NULL), [alias] [varchar](45)
		 * NULL DEFAULT (NULL), [dtmDateStamp] [datetime] NULL DEFAULT (NULL) ) ON
		 * [PRIMARY]
		 */

		try {

			/* trim old and new value to 100 chars */
			strOldValue = strOldValue.substring(0, Math.min(strOldValue.length(), 99)).trim();
			strNewValue = strNewValue.substring(0, Math.min(strNewValue.length(), 99)).trim();

			Statement st = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			String strSQLCommand = "";
			try {

				switch (DBConnection.dbType) {
				case "MYSQL":
					strSQLCommand = " INSERT INTO log_audit (table_name, key_value, column_name, "
							+ " old_value, new_value, alias, dtmDateStamp ) " + "	values( '" + strTableName + "','"
							+ strKeyValue + "','" + strColumnName + "', " + " '" + strOldValue + "','" + strNewValue
							+ "','" + oUser.getString("ALIAS") + "', now())";
					break;
				case "MSSQL":
					strSQLCommand = " INSERT INTO log_audit (table_name, key_value, column_name, "
							+ " old_value, new_value, alias, dtmDateStamp ) " + "	values( '" + strTableName + "','"
							+ strKeyValue + "','" + strColumnName + "', " + " '" + strOldValue + "','" + strNewValue
							+ "','" + oUser.getString("ALIAS") + "', getdate())";
					break;
				case "H2":
					strSQLCommand = " INSERT INTO log_audit (table_name, key_value, column_name, "
							+ " old_value, new_value, alias, dtmDateStamp ) " + "	values( '" + strTableName + "','"
							+ strKeyValue + "','" + strColumnName + "', " + " '" + strOldValue + "','" + strNewValue
							+ "','" + oUser.getString("ALIAS") + "', getdate())";
					break;
				}

				st.execute(strSQLCommand);

			} catch (Exception e) {
				logger.info("WriteLog ... insert in log_audit");
				logger.info(e.getMessage());
				e.printStackTrace();
			}

		} catch (SQLException e) {
			logger.info("WriteLog ... create statement");
			logger.info(e.getMessage());
			e.printStackTrace();
		}

		con.ReleaseMe();
		return;

	}

	/**
	 * return the column Type (NUMERIC, DOUBLE, STRING , etc ... )
	 * 
	 * @param intColumnType
	 * @return
	 */
	public String getFieldType(int intColumnType) {
		String strType = "";

		switch (intColumnType) {
		// numeric
		case Types.TINYINT:
			// 1;
		case Types.SMALLINT:
			// 2;
		case Types.INTEGER:
			// 3
			strType = "NUMERIC";
			break;
		case Types.FLOAT:
			// 4;
			strType = "FLOAT";
			break;
		case Types.DOUBLE:
			// 5;
			strType = "DOUBLE";
			break;
		// varchar sau text sau char sau nchar
		case -15:
		case 12:
		case -1:
		case 1:
			strType = "STRING";
			break;
		// bit
		case -7:
			strType = "BOOLEAN";
			break;
		// date
		case 91:
			strType = "DATE";
			break;
		// smalldatetime
		case 93:
			strType = "DATE";
			break;
		default:
			strType = "NOT DEFINED";
		}
		return strType;

	}

	/**
	 * Generate a blank (empty) record
	 * 
	 * @param oUser
	 * @param oRecord
	 * @param tableName
	 * @param colName
	 * @param colValue
	 * @param colKeyName
	 */
	public DBRecord GetBlankDBRecord(DBRecord oUser, String tableName, String colName, String colValue,
			String colKeyName) {

		DBRecord oRecord = new DBRecord();
		DBTable TStruct = new DBTable();

		// read the table structure of the table
		String strSQLCommand = "";

		switch (DBConnection.dbType) {
		case "MYSQL":
			strSQLCommand = "describe " + tableName;
			break;
		case "MSSQL":
			// @formatter:off
			strSQLCommand = "SELECT [column].COLUMN_NAME, [column].DATA_TYPE, COLUMNPROPERTY(object_id([column].[TABLE_NAME]),"
					+ " [column].[COLUMN_NAME], 'IsIdentity') AS [identity]"
					+ " FROM INFORMATION_SCHEMA.COLUMNS [column]  WHERE [column].[Table_Name] = '" + tableName
					+ "';";
			// @formatter:on
			break;
		case "H2":
			strSQLCommand = "show columns from " + tableName;
			break;
		}

		TStruct = getDBTable(null, strSQLCommand);
		oRecord.isNew = true;
		oRecord.tableName = tableName;
		oRecord.KeyName = colName;

		if (!colValue.isEmpty())
			oRecord.KeyValue = colValue;

		else if (!colKeyName.isEmpty())
			if (DBConnection.dbType.equals("MSSQL")) {
				// TODO - GetNNewID
				// oRecord.KeyValue = GETNNEWID(oUser, colKeyName, tableName);
			}

		String strColname = null, strType = null;

		DBType type = new DBType();

		for (int i = 0; i < TStruct.reccount(); i++) {
			/* generate the result */
			switch (DBConnection.dbType) {
			case "MYSQL":
				strColname = TStruct.get(i).getString("COLUMN_NAME").toUpperCase().trim();
				strType = TStruct.get(i).getString("COLUMN_TYPE").toUpperCase().trim();
				type = InterpretType(strType);
				type.columnName = strColname;
				type.autoincrement = TStruct.get(i).get("EXTRA").toString().toUpperCase().trim()
						.equals("AUTO_INCREMENT");
				oRecord.dbStructure.add(type);
				break;

			case "MSSQL":
				strColname = TStruct.get(i).getString("COLUMN_NAME").toUpperCase().trim();
				strType = TStruct.get(i).getString("DATA_TYPE").toUpperCase().trim();
				type = InterpretType(strType);
				type.columnName = strColname;
				type.autoincrement = TStruct.get(i).get("IDENTITY").toString().toUpperCase().trim().equals("1");
				oRecord.dbStructure.add(type);
				break;

			case "H2":
				strColname = TStruct.get(i).getString("COLUMN_NAME").toUpperCase().trim();
				strType = TStruct.get(i).getString("TYPE").toUpperCase().trim();
				type = InterpretType(strType);
				type.columnName = strColname;
				type.autoincrement = (!TStruct.get(i).get("DEFAULT").toString().toUpperCase().trim().equals("NULL"));
				oRecord.dbStructure.add(type);
				break;

			}

			oRecord.put(strColname, type.defaultValue);

		}

		oRecord.isChanged = false;

		return oRecord;

	}

	/**
	 * Generate the type name and the type default value
	 * 
	 * @param pStrType
	 * @return
	 */
	private DBType InterpretType(String pStrType) {
		DBType type = new DBType();

		pStrType = pStrType.toUpperCase();

		if (pStrType.contains("BIT") || pStrType.contains("TINYINT(1)")) {
			type.columnType = "BOOLEAN";
			type.defaultValue = false;
			return type;
		}

		if (pStrType.contains("CHAR") || pStrType.contains("TEXT")) {
			type.columnType = "STRING";
			type.defaultValue = "";
			return type;
		}

		if (pStrType.contains("BIGINT")) {
			type.columnType = "BIGINT";
			type.defaultValue = 0;
			return type;
		}
		if (pStrType.contains("INT") || pStrType.contains("TINYINT(4)") || pStrType.contains("SMALLINT")) {
			type.columnType = "INT";
			type.defaultValue = 0;
			return type;
		}
		if (pStrType.contains("DOUBLE")) {
			type.columnType = "DOUBLE";
			type.defaultValue = 0;
			return type;
		}

		if (pStrType.contains("FLOAT")) {
			type.columnType = "FLOAT";
			type.defaultValue = 0;
			return type;
		}

		if (pStrType.contains("DECIMAL")) {
			type.columnType = "DECIMAL";
			type.defaultValue = 0;
			return type;
		}

		if (pStrType.contains("DATE")) {
			type.columnType = "DATE";
			type.defaultValue = null;
			return type;
		}

		if (pStrType.contains("TIME")) {
			type.columnType = "TIME";
			type.defaultValue = null;
			return type;
		}

		if (pStrType.contains("BLOB")) {
			type.columnType = "BLOB";
			type.defaultValue = null;
			return type;
		}
		logger.error("GetBlankRecord - type not defined!");
		logger.info(pStrType);
		return null;
	}

	/**
	 * Generate the type name and the type default value
	 * 
	 * @param pStrType
	 * @return
	 * @throws Exception
	 */
	private DBType InterpretType(int pnType) throws Exception {
		DBType type = new DBType();

		switch (pnType) {
		// numeric
		case 4:
		case 5:
			type.columnType = "INT";
			type.defaultValue = 0;
			break;
		case 2:
		case 3:
			type.columnType = "DOUBLE";
			type.defaultValue = 0;
			break;
		// biginteger
		case -5:
			type.columnType = "BIGINT";
			type.defaultValue = 0;
			break;
		// varchar sau text sau char sau nchar
		case -15:
		case 12:
		case -9:
		case -1:
		case 1:
			type.columnType = "STRING";
			type.defaultValue = "";
			break;
		// bit
		case -7:
			type.columnType = "BOOLEAN";
			type.defaultValue = false;
			break;
		// date
		case 91:
		case 93:
			type.columnType = "DATE";
			type.defaultValue = null;
			break;

		// unknown
		default:
			type.columnType = "<UNKNOW>";
			type.defaultValue = null;
			logger.error("InterpretType - type not defined!");
			logger.error("code: " + pnType);
			throw new Exception("InterpretType - type not defined!");

		}// strColname!="RECORD"

		return type;
	}

	/**
	 * return in specific type from an RS
	 * 
	 * @param pRs
	 * @param pType
	 * @return
	 * @throws Exception
	 */
	Object getFromRS(ResultSet pRs, DBType pType) throws Exception {
		switch (pType.columnType) {
		case "STRING":
			return pRs.getString(pType.columnName);

		case "BIGINT":
			return pRs.getBigDecimal(pType.columnName);

		case "INT":
			return pRs.getInt(pType.columnName);

		case "DOUBLE":
			return pRs.getDouble(pType.columnName);

		case "BOOLEAN":
			return pRs.getBoolean(pType.columnName);

		case "DATE":
			java.util.Calendar cal = Calendar.getInstance();
			cal.setTimeZone(java.util.TimeZone.getDefault());
			return pRs.getDate(pType.columnName, cal);

		case "DATETIME":
			java.util.Calendar cal1 = Calendar.getInstance();
			cal1.setTimeZone(java.util.TimeZone.getDefault());
			return pRs.getTimestamp(pType.columnName, cal1);

		default:
			logger.error("getFromRS - type not defined!");
			logger.error(pType.columnName);
			logger.error(pType.columnType);
			throw new Exception(
					"getFromRS - type not defined: " + pType.columnName + " type:" + pType.columnType + ":");

		}// switch
	}

	/**
	 * 
	 * return several result sets
	 * 
	 * @param user
	 * @param p_nTables
	 * @param oListDB
	 * @param p_strSQLCommand
	 * @return
	 */
	public List<DBTable> getDBXTable(DBRecord oUser, String p_strSQLCommand) {

		List<DBTable> oListDB = new ArrayList<DBTable>();
		String strErrorMessage = "";

		// connection
		DBConnection con = this.getConn(oUser);
		Connection conn = con.con;

		try {

			Statement stmt = conn.createStatement();

			boolean results = false;

			// call
			try {
				results = stmt.execute(p_strSQLCommand);
				// logger.info(p_strSQLCommand);
			} catch (Exception e) {
				logger.info("GetDBXTable ... execute");
				logger.info(p_strSQLCommand);
				logger.info(e.toString());
				strErrorMessage = e.toString();

			}

			// Loop through the available result sets.
			int nresultsetno = 0;
			do {
				if (results) {
					ResultSet rs = stmt.getResultSet();

					// Show data from the result set.
					// logger.info("RESULT SET #" + rsCount);

					// the table
					// DBTable
					DBTable oTable = new DBTable();
					// logger.info("results cycle");
					// logger.info(nresultsetno);
					// no table name
					oTable.tableName = "" + nresultsetno;
					nresultsetno++;
					// no key name
					oTable.KeyName = "";

					boolean bFirstTime = true;
					int nrecno = 0;
					if (rs != null) {
						while (rs.next()) {
							// generate each record and put it in the list
							ResultSetMetaData rsmdResult = null;

							// logger.info("records cycle");
							// logger.info(nrecno);

							int intNoCols = 0;
							String strColname = null;
							Object strColvalue = null;
							DBRecord oDBRecord = new DBRecord();
							nrecno++;
							oDBRecord.recno = nrecno;
							try {
								rsmdResult = rs.getMetaData();
								intNoCols = rsmdResult.getColumnCount();

								oDBRecord.tableName = "";
								oDBRecord.KeyName = "";
								oDBRecord.KeyValue = "";

								try {
									for (int intCount = 1; intCount <= intNoCols; intCount++) {

										// save the fields names
										if (bFirstTime) {
											oTable.Fields.add(strColname);
											oTable.FieldTypes.add(getFieldType(rsmdResult.getColumnType(intCount)));
										}

										strColname = rsmdResult.getColumnName(intCount);
										// logger.info(strColname);

										// NOTE: THE COLUMN NAMES WILL ALWAYS BE
										// STORED IN
										// UPPERCASE, HENCE NEED TO BE RETRIEVED
										// IN UPPER
										// CASE
										strColname = strColname.toUpperCase();
										strColvalue = rs.getString(strColname);

										oDBRecord.put_original(strColname.toUpperCase(), strColvalue);

										// NOTE: THE COLUMN NAMES WILL ALWAYS BE
										// STORED IN
										// UPPERCASE, HENCE NEED TO BE RETRIEVED
										// IN UPPER
										// CASE
									} // for
								} catch (Exception e) {
									logger.info("GetDBXTable fill DBRecord... for ");
									logger.info(e.getMessage());
									e.printStackTrace();
									strErrorMessage = e.toString();
								}

								bFirstTime = false;

								// key value
								oDBRecord.KeyValue = (String) oDBRecord.get(oDBRecord.KeyName);

								// add in Table
								oTable.add(oDBRecord);

							} catch (SQLException e) {
								logger.error("GetDBXTable fill DBRecord... body");
								logger.info(e.getMessage());
								e.printStackTrace();
								strErrorMessage = e.toString();
							} // get Meta Data

						} // while

						// add the table to the List
						oListDB.add(oTable);
						// close the result set
						rs.close();

					} // if != null

				} // if(results)

				// next resultset
				results = stmt.getMoreResults();

				// logger.info("Result set step ");
				// logger.info(nresultsetno);

			} while (results);

		} catch (SQLException e) {
			logger.error("GetDBXTable ... get connection");
			logger.info(e.getMessage());
			e.printStackTrace();
			strErrorMessage = e.toString();
		} // try general

		oListDB.get(0).strErrorMessage = strErrorMessage;
		return oListDB;
	}

	/**
	 * set the user credentials into the SPID table
	 * 
	 * @param oUser
	 * @param conn
	 */
	public void SetUserSPID(DBRecord oUser, Connection conn) {

		if (oUser == null)
			return;

		/*
		 * if the connection is succesfull and TheApp.loginInfo.R is not null - set the
		 * information in UserSpid
		 */
		// logger.info("Set user SPID from DB");

		DBConnection con = null;

		if (conn == null) {
			con = this.getConn(oUser);
			conn = con.con;
		}

		// logger.info("--------------------------------------------------");
		// logger.info(oUser);
		// logger.info("--------------------------------------------------");

		if (oUser != null) {
			if (!oUser.tableName.isEmpty()) {
				String UserID = oUser.getString("USERID");
				// logger.info(oUser);
				// logger.info(UserID);

				try {

					Statement st = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
					String strSQLCommand = "";
					try {

						switch (DBConnection.dbType) {
						case "MYSQL":
							strSQLCommand = " CREATE TABLE IF NOT EXISTS userspid (userid VARCHAR(10), spid INT);";
							st.execute(strSQLCommand);
							strSQLCommand = " INSERT INTO userspid values( '" + UserID + "',connection_id());";
							break;
						case "MSSQL":
							strSQLCommand = "IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_NAME='userspid')";
							strSQLCommand += " BEGIN ";
							strSQLCommand += " DELETE FROM userspid WHERE spid = @@SPID ";
							strSQLCommand += " IF NOT EXISTS (SELECT 1 FROM userspid WHERE spid = @@SPID AND userid = '"
									+ UserID + "')";
							strSQLCommand += "  INSERT INTO userspid values( '" + UserID + "',@@SPID)";
							strSQLCommand += " END ";
							break;
						case "H2":
							// TODO - SPID H2
							break;
						}

						st.execute(strSQLCommand);
						// logger.info("SPID - set !");

					} catch (Exception e) {
						logger.info("SetUserSPID ... insert in UserSPID");
						logger.info(e.toString());
					}

				} catch (SQLException e) {
					logger.info("SetUserSPID ... create statement");
					logger.info(e.getMessage());
					e.printStackTrace();
				}
			} else {
				// logger.info("SetUserSPID ... user not logged !");
			}
		} else {
			logger.info("User is null !");
		}

		if (con != null)
			con.ReleaseMe();
	}

}
