package ro.fagadar.daylight;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class DBTable implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2264335600307875360L;

	public List<DBRecord> Records;
	public List<String> Fields;
	public List<String> FieldTypes;
	public List<DBRecord> DeletedRecords;
	// public ArrayList<DBRecord> Records;
	public String tableName = "";
	public String KeyName = "";
	public int nLastLocatePos = -1;

	/**
	 * blank constructor
	 */
	public DBTable() {
		DBTable.this.Records = new ArrayList<DBRecord>();
		DBTable.this.DeletedRecords = new ArrayList<DBRecord>();
		DBTable.this.Fields = new ArrayList<String>();
		DBTable.this.FieldTypes = new ArrayList<String>();
	}

	/**
	 * add record
	 * 
	 * @param oDBRecord
	 */
	public void add(DBRecord oDBRecord) {
		// add one record in the table
		DBTable.this.Records.add(oDBRecord);
	}

	/**
	 * add record on a specified position
	 * 
	 * @param index
	 * @param oDBRecord
	 */
	public void add(int index, DBRecord oDBRecord) {
		// add one record in the table
		DBTable.this.Records.add(index, oDBRecord);
	}

	/**
	 * return record
	 * 
	 * @param i
	 * @return
	 */
	public DBRecord get(int i) {
		// get one record
		return DBTable.this.Records.get(i);
	}

	/**
	 * set record on a specified position
	 * 
	 * @param R
	 * @param i
	 */
	public void set(DBRecord R, int i) {
		// set a value for one record
		DBTable.this.Records.set(i, R);
	}

	/**
	 * delete record
	 * 
	 * @param i
	 */
	public void delete(int i) {
		// delete one record
		// move him in the DeletedRecords if it-s not new
		DBRecord R = Records.get(i);
		if (!R.isNew)
			DBTable.this.DeletedRecords.add(R);
		DBTable.this.Records.remove(i);
	}

	/**
	 * delete all
	 */
	public void clear() {
		DBTable.this.Records.clear();
	}

	/**
	 * record count
	 * 
	 * @return
	 */
	public int reccount() {
		return DBTable.this.Records.size();
	}

	/**
	 * record count for a specified condition
	 * 
	 * @param strFieldName
	 * @param strValue
	 * @return
	 */
	public int count(String strFieldName, String strValue) {
		int nCount = 0;
		for (int i = 0; i < this.reccount(); i++)
			if (strValue.trim().equals(this.get(i).get(strFieldName.toUpperCase()).toString().trim())) {
				nCount++;
			}

		return nCount;
	}

	/**
	 * search by a field (after the first search use continue(with the same
	 * parameters))
	 * 
	 * @param strFieldName
	 * @param strValue
	 * @return
	 */
	public DBRecord Locate(String strFieldName, String strValue) {
		for (int i = 0; i < this.reccount(); i++)
			if (strValue.trim().equals(this.get(i).get(strFieldName.toUpperCase()).toString().trim())) {
				nLastLocatePos = i;
				return this.get(i);
			}
		nLastLocatePos = -1;
		return null;
	}

	/**
	 * continue after search
	 * 
	 * @param strFieldName
	 * @param strValue
	 * @return
	 */
	public DBRecord Continue(String strFieldName, String strValue) {
		for (int i = this.nLastLocatePos == -1 ? 0 : this.nLastLocatePos; i < this.reccount(); i++)
			if (strValue.trim().equals(this.get(i).get(strFieldName.toUpperCase()).toString().trim())) {
				nLastLocatePos = i;
				return this.get(i);
			}
		nLastLocatePos = -1;
		return null;
	}

	/**
	 * search for two fields (after search use continue)
	 * 
	 * @param strFieldName
	 * @param strValue
	 * @param strFieldName1
	 * @param strValue1
	 * @return
	 */
	public DBRecord Locate(String strFieldName, String strValue, String strFieldName1, String strValue1) {
		for (int i = 0; i < this.reccount(); i++)
			if (strValue.trim().equals(this.get(i).get(strFieldName).toString().trim())
					&& strValue1.trim().equals(this.get(i).get(strFieldName1.toUpperCase()).toString().trim())) {
				nLastLocatePos = i;
				return this.get(i);
			}
		return null;
	}

	/**
	 * continue after search of two fields
	 * 
	 * @param strFieldName
	 * @param strValue
	 * @param strFieldName1
	 * @param strValue1
	 * @return
	 */
	public DBRecord Continue(String strFieldName, String strValue, String strFieldName1, String strValue1) {
		for (int i = this.nLastLocatePos == -1 ? 0 : this.nLastLocatePos; i < this.reccount(); i++)
			if (strValue.trim().equals(this.get(i).get(strFieldName.toUpperCase()).toString().trim())
					&& strValue1.trim().equals(this.get(i).get(strFieldName1.toUpperCase()).toString().trim())) {
				nLastLocatePos = i;
				return this.get(i);
			}
		nLastLocatePos = -1;
		return null;
	}

	/**
	 * search with two fields from the last position
	 * 
	 * @param strFieldName
	 * @param strValue
	 * @param strFieldName1
	 * @param strValue1
	 * @return
	 */
	public DBRecord LocateFromPos(String strFieldName, String strValue, String strFieldName1, String strValue1) {
		for (int i = nLastLocatePos; i < this.reccount(); i++)
			if (strValue.trim().equals(this.get(i).get(strFieldName.toUpperCase()).toString().trim())
					&& strValue1.trim().equals(this.get(i).get(strFieldName1.toUpperCase()).toString().trim())) {
				nLastLocatePos = i + 1;
				return this.get(i);
			}
		nLastLocatePos = this.reccount();
		return null;
	}

	/**
	 * search for one field from the last position
	 * 
	 * @param strFieldName
	 * @param strValue
	 * @return
	 */
	public DBRecord LocateFromPos(String strFieldName, String strValue) {
		for (int i = nLastLocatePos; i < this.reccount(); i++)
			if (strValue.trim().equals(this.get(i).get(strFieldName.toUpperCase()).toString().trim())) {
				nLastLocatePos = i + 1;
				return this.get(i);
			}
		nLastLocatePos = this.reccount();
		return null;
	}

	/**
	 * Select info for field=value
	 * 
	 * @param strFieldName
	 * @param strValue
	 * @return
	 */
	public DBTable Select(String strFieldName, String strValue) {

		DBTable Destination = new DBTable();

		for (int i = 0; i < this.Fields.size(); i++)
			Destination.Fields.add(this.Fields.get(i));

		for (int i = 0; i < this.FieldTypes.size(); i++)
			Destination.FieldTypes.add(this.FieldTypes.get(i));

		this.nLastLocatePos = 0;
		for (int i = 0; i < this.reccount(); i++) {
			DBRecord R = null;
			R = this.LocateFromPos(strFieldName, strValue);
			if (R == null) {
				// nothing
			} else
				Destination.add(R);
		}
		return Destination;
	}

	/**
	 * Select unique field (only one time)
	 * 
	 * @param strFieldName
	 * @return
	 */
	public DBTable SelectGroup(String strFieldName) {

		DBTable Destination = new DBTable();

		for (int i = 0; i < this.Fields.size(); i++)
			Destination.Fields.add(this.Fields.get(i));

		for (int i = 0; i < this.FieldTypes.size(); i++)
			Destination.FieldTypes.add(this.FieldTypes.get(i));

		for (int i = 0; i < this.reccount(); i++) {
			DBRecord R = new DBRecord();
			R.put(strFieldName, this.get(i).getString(strFieldName));
			if (null == Destination.Locate(strFieldName, R.getString(strFieldName)))
				Destination.add(R);
		} // for
		Destination.Sort(strFieldName, "String", 1);

		return Destination;
	}

	/**
	 * Select info for field1=value1 and field2=value2
	 * 
	 * @param strFieldName
	 * @param strValue
	 * @param strFieldName1
	 * @param strValue1
	 * @return
	 */
	public DBTable Select(String strFieldName, String strValue, String strFieldName1, String strValue1) {

		DBTable Destination = new DBTable();

		for (int i = 0; i < this.Fields.size(); i++)
			Destination.Fields.add(this.Fields.get(i));

		for (int i = 0; i < this.FieldTypes.size(); i++)
			Destination.FieldTypes.add(this.FieldTypes.get(i));

		this.nLastLocatePos = 0;
		for (int i = 0; i < this.reccount(); i++) {
			DBRecord R = null;
			R = this.LocateFromPos(strFieldName, strValue, strFieldName1, strValue1);
			if (R == null) {
				// nothing
			} else
				Destination.add(R);
		}
		return Destination;
	}

	/**
	 * Sort for a specified field and type
	 * 
	 * @param strFieldName
	 * @param type
	 *            (String, Integer)
	 * @param sens
	 */
	public void Sort(String strFieldName, String type, int sens) {

		// bubble sort
		for (int i = 0; i < this.reccount() - 1; i++)
			for (int j = i + 1; j < this.reccount(); j++) {
				if ("String".equals(type)) {
					if (this.get(i).getString(strFieldName.toUpperCase())
							.compareToIgnoreCase(this.get(j).getString(strFieldName.toUpperCase())) * sens > 0) {
						// switch between records
						DBRecord R;
						R = this.get(i);
						this.set(this.get(j), i);
						this.set(R, j);
					}
				} // String
				if ("Integer".equals(type)) {
					if ((this.get(i).getInteger(strFieldName.toUpperCase())
							- this.get(j).getInteger(strFieldName.toUpperCase())) * sens > 0) {
						// switch between records
						DBRecord R;
						R = this.get(i);
						this.set(this.get(j), i);
						this.set(R, j);
					}
				} // Integer

			} // for for

	}

	/**
	 * Sum of a field
	 * 
	 * @param strFieldName
	 * @return
	 */
	public double sum(String strFieldName) {
		double nSum = 0d;
		for (int i = 0; i < this.reccount(); i++)
			nSum += this.get(i).getDouble(strFieldName.toUpperCase());

		return nSum;
	}

	/**
	 * Sum field1*field2
	 * 
	 * @param strFieldName1
	 * @param strFieldName2
	 * @return
	 */
	public double sum(String strFieldName1, String strFieldName2) {
		double nSum = 0d;
		for (int i = 0; i < this.reccount(); i++)
			nSum += (this.get(i).getDouble(strFieldName1.toUpperCase())
					* this.get(i).getDouble(strFieldName2.toUpperCase()));

		return nSum;
	}

	/**
	 * MAX (numeric)
	 * 
	 * @param strKey
	 * @return
	 */
	public DBRecord max(String strKey) {
		DBRecord R = null;

		double max, buffer;

		max = this.get(0).getDouble(strKey);
		buffer = this.get(0).getDouble(strKey);
		R = this.get(0);

		for (int i = 0; i < this.reccount(); i++) {
			buffer = this.get(i).getDouble(strKey);
			if (max < buffer) {
				max = buffer;
				R = this.get(i);
			}
		}

		return R;
	}

	/**
	 * MAX (String)
	 * 
	 * @param strKey
	 * @return
	 */
	public DBRecord maxString(String strKey) {
		DBRecord R = null;

		String max, buffer;
		max = this.get(0).getString(strKey);
		buffer = this.get(0).getString(strKey);
		R = this.get(0);

		for (int i = 0; i < this.reccount(); i++) {
			buffer = this.get(i).getString(strKey);
			if (max.compareToIgnoreCase(buffer) < 0) {
				max = buffer;
				R = this.get(i);
			}
		}

		return R;
	}

	/**
	 * MIN (numeric)
	 * 
	 * @param strKey
	 * @return
	 */
	public DBRecord min(String strKey) {
		DBRecord R = null;

		double min, buffer;

		min = this.get(0).getDouble(strKey);
		buffer = this.get(0).getDouble(strKey);
		R = this.get(0);

		for (int i = 0; i < this.reccount(); i++) {
			buffer = this.get(i).getDouble(strKey);
			if (min > buffer) {
				min = buffer;
				R = this.get(i);
			}
		}

		return R;
	}

	/**
	 * MIN (string)
	 * 
	 * @param strKey
	 * @return
	 */
	public DBRecord minString(String strKey) {
		DBRecord R = null;

		String min, buffer;
		min = this.get(0).getString(strKey);
		buffer = this.get(0).getString(strKey);
		R = this.get(0);

		for (int i = 0; i < this.reccount(); i++) {
			buffer = this.get(i).getString(strKey);
			if (min.compareToIgnoreCase(buffer) > 0) {
				min = buffer;
				R = this.get(i);
			}
		}

		return R;
	}

	@Override
	public String toString() {

		String strReturn = "DBTable->";
		strReturn += " tableName:";
		strReturn += tableName;
		strReturn += " KeyName:";
		strReturn += KeyName;

		for (int i = 0; i < this.reccount(); i++) {
			strReturn += "\n";
			strReturn += this.get(i).toString();
		}
		return strReturn;

	}

}
