package ro.fagadar.daylight;

import java.util.ArrayList;

public class DBStructure extends ArrayList<DBType> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	String getFieldsForInsert() {
		String strFields = "";
		for (int i = 0; i < this.size(); i++) {
			if (!this.get(i).autoincrement) {
				strFields += this.get(i).columnName;
				strFields += (i == this.size() - 1) ? "" : ",";
			}
		}
		return strFields;
	}

	public String getQuestionMarksForSet() {
		String strFields = "";
		for (int i = 0; i < this.size(); i++) {
			if (!this.get(i).autoincrement) {
				strFields += "?";
				strFields += (i == this.size() - 1) ? "" : ",";
			}
		}
		return strFields;

	}

	String getFieldsForSet() {
		String strFields = "";
		for (int i = 0; i < this.size(); i++) {
			if (!this.get(i).autoincrement) {
				strFields += this.get(i).columnName;
				strFields += "=? ";
				strFields += (i == this.size() - 1) ? "" : ",";
			}
		}
		return strFields;
	}

}
