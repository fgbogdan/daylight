package ro.fagadar.daylight;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

public class DBRecord extends HashMap<Object, Object> implements Serializable {

	/**
	 * 
	 */
	final static Logger logger = Logger.getLogger(DBRecord.class);
	private static final long serialVersionUID = -6847908896735454646L;
	public String tableName = "";
	public String KeyName = "";
	public String KeyValue = "";
	public boolean isNew = false;
	public boolean isFirst = false;
	public boolean isDeleted = false;
	public boolean isChanged = false;

	// log changes
	public boolean isLog = false;
	public int recno = 1;

	// for serialisation compiler problems
	Double dbldummy;
	Float fltdummy;
	Boolean bdummy;
	Integer idummy;
	java.sql.Date ddummy;
	java.sql.Timestamp tdummy;

	DBStructure dbStructure = new DBStructure();

	// blank constructor
	public DBRecord() {
	}

	// constructor with log
	public DBRecord(boolean isLog) {
		// logging
		this.isLog = isLog;
	}

	// overridde put
	public void put(String key, Object o) {
		// normal
		// change (isChanged only if different
		Object oldObject = super.get(key);
		String oldVal, newVal;
		if (oldObject != null && o != null) {
			oldVal = oldObject.toString().trim();
			newVal = o.toString().trim();
			if (!oldVal.equals(newVal))
				this.isChanged = true;
		} else
			this.isChanged = true;
		// put
		super.put(key.toUpperCase(), o);

	}

	// put from DB (first time)
	public void put_original(String key, Object o) {
		// logging
		if (this.isLog)
			super.put(key.toUpperCase() + "_ORIGINAL", o);
		// normal
		super.put(key, o);
	}

	// put nochange
	public void put_nochange(String key, Object o) {
		super.put(key.toUpperCase(), o);
	}

	// recno
	public int recno() {
		return this.recno;
	}

	@SuppressWarnings("rawtypes")
	public void CopyTo(DBRecord Destination) {

		// properties
		Destination.tableName = this.tableName;
		Destination.KeyName = this.KeyName;
		Destination.KeyValue = this.KeyValue;
		Destination.isNew = this.isNew;
		Destination.isDeleted = this.isDeleted;
		Destination.isChanged = this.isChanged;
		Destination.isFirst = this.isFirst;
		Destination.isLog = this.isLog;
		Destination.recno = this.recno;

		// Create a Set with the keys in the HashMap.
		Set set = this.keySet();
		// Iterate over the Set to see what it contains.
		Iterator iter = set.iterator();
		while (iter.hasNext()) {
			Object o = iter.next();
			String strKey = o.toString();
			Destination.put(strKey.toUpperCase(), this.get(strKey.toUpperCase()));
		}
	} // CopyTo

	// getString
	public String getString(String strKey) {
		return this.get(strKey.toUpperCase()).toString();
	}

	// getStringNotZero
	public String getStringNotZero(String strKey) {
		if (this.getDouble(strKey.toUpperCase()) == 0d)
			return "";
		else
			return this.getString(strKey.toUpperCase());
	}

	// getDouble
	public Double getDouble(String strKey) {
		return Double.valueOf(this.get(strKey.toUpperCase()).toString());
	}

	// getFloat
	public Float getFloat(String strKey) {
		return Float.valueOf(this.get(strKey.toUpperCase()).toString());
	}

	// getInteger
	public int getInteger(String strKey) {
		return Integer.parseInt(this.get(strKey.toUpperCase()).toString());
	}

	// getBoolean
	public boolean getBoolean(String strKey) {
		String strColvalue = null;
		strColvalue = this.get(strKey.toUpperCase()).toString().trim();
		boolean lRetVal;
		if ("0".equals(strColvalue) || "false".equals(strColvalue))
			lRetVal = false;
		else
			lRetVal = true;
		// return Boolean.valueOf(strColvalue);
		return lRetVal;

	}

	public Date getDate(String strKey) {

		Object o = this.get(strKey.toUpperCase());
		if (o == null)
			return null;
		String strDate = o.toString();
		return DateUtils.String2Date(strDate, "yyyy-MM-dd");
	}

	public String getDateString(String strKey) {

		Object o = this.get(strKey.toUpperCase());
		if (o == null)
			return null;
		String strDate = o.toString();
		return DateUtils.Date2String(DateUtils.String2Date(strDate, "yyyy-MM-dd"), "yyyy-MM-dd");
	}

	public String getDateString(String strKey, String strFormat) {

		Object o = this.get(strKey.toUpperCase());
		if (o == null)
			return null;
		String strDate = o.toString();
		return DateUtils.Date2String(DateUtils.String2Date(strDate, "yyyy-MM-dd"), strFormat);
	}

	@Override
	public String toString() {
		String strReturn = "DBRecord->";
		strReturn += " tableName:";
		strReturn += tableName;
		strReturn += " recno:";
		strReturn += recno;
		strReturn += " KeyName:";
		strReturn += KeyName;
		strReturn += " KeyValue:";
		strReturn += KeyValue;
		strReturn += " isNew:";
		strReturn += isNew ? "yes" : "no";
		strReturn += " isFirst:";
		strReturn += isFirst ? "yes" : "no";
		strReturn += " isDeleted:";
		strReturn += isDeleted ? "yes" : "no";
		strReturn += " isChanged:";
		strReturn += isChanged ? "yes" : "no";
		strReturn += " isLog:";
		strReturn += isLog ? "yes" : "no";
		strReturn += " values:";
		strReturn += super.toString();
		strReturn += " fields:";
		for (DBType t : dbStructure) {
			strReturn += t.toString();
		}

		return strReturn;

	}

	int getNrColumns() {
		return this.dbStructure.size();
	}

	public void setPreparedStatementValues(PreparedStatement ps) {

		// set values for each column
		try {
			String columnName;
			int nFieldOffset = 1;
			for (int i = 0; i < this.getNrColumns(); i++) {

				// if we hit the autoincrement field - nFieldOffset became lower with 1
				if (this.dbStructure.get(i).autoincrement) {
					nFieldOffset--;
				} else {
					columnName = this.dbStructure.get(i).columnName;
					switch (this.dbStructure.get(i).columnType) {

					case "STRING":
						ps.setString(i + nFieldOffset, this.getString(columnName));
						break;

					case "BIGINT":
						ps.setBigDecimal(i + nFieldOffset, new BigDecimal(this.getString(columnName)));
						break;

					case "INT":
						ps.setInt(i + nFieldOffset, this.getInteger(columnName));
						break;

					case "DOUBLE":
						ps.setDouble(i + nFieldOffset, this.getDouble(columnName));
						break;

					case "FLOAT":
						ps.setFloat(i + nFieldOffset, this.getFloat(columnName));
						break;

					case "DECIMAL":
						ps.setBigDecimal(i + nFieldOffset, new BigDecimal(getString(columnName)));
						break;

					case "BOOLEAN":
						ps.setBoolean(i + nFieldOffset, this.getBoolean(columnName));
						break;

					case "DATE":
					case "TIME":
					case "DATETIME":
						ps.setDate(i + nFieldOffset, (java.sql.Date) this.getDate(columnName));
						break;

					case "BLOB":
						ps.setBlob(i + nFieldOffset, null, 1);
						break;
					default:
						logger.error("setPreparedStatement - type not defined!");
						logger.error(this.dbStructure.get(i).columnName);
						logger.error(this.dbStructure.get(i).columnType);

					} // switch
				} // if autoincrement
			} // for.exe.executeQuery(sql)

			// only for update
			if (!this.isNew) {
				ps.setString(this.getNrColumns() + nFieldOffset, this.KeyValue);
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

	}

}
