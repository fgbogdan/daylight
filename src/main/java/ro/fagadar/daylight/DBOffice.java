
package ro.fagadar.daylight;

import java.awt.Desktop;
import java.awt.print.PrinterJob;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.printing.PDFPageable;
import org.apache.poi.hssf.usermodel.HSSFFormulaEvaluator;
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFFormulaEvaluator;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import net.sf.jasperreports.engine.JRException;
import net.sf.jasperreports.engine.JRExporterParameter;
import net.sf.jasperreports.engine.JasperExportManager;
import net.sf.jasperreports.engine.JasperFillManager;
import net.sf.jasperreports.engine.JasperPrint;
import net.sf.jasperreports.engine.export.JRXlsExporter;
import net.sf.jasperreports.engine.export.JRXlsExporterParameter;


public class DBOffice {
	
	public DBRecord getUser() {
//		HttpServletRequest request = this.getThreadLocalRequest();
//		HttpSession session = request.getSession();
//
//		return (DBRecord) session.getAttribute("User");
		//TODO - return user
		return null;

	}

	
	
	public String getReport(String fileName, HashMap<String, Object> param, String type) {
		try {
			String filesRepository = DBConnection.FilesRepository + "/REPORTS/";
			Connection con = DbManager.getDB().getConn(getUser()).con;
			String fileNamewithPath = "";
			// check for report file in files repository
			if (new File(filesRepository + fileName + ".jasper").exists()) {
				fileNamewithPath += filesRepository + fileName;
			} else {
				// use war/reports path
				fileNamewithPath = "no file ";
			}

			// define the report
			JasperPrint jasperPrint = JasperFillManager.fillReport(fileNamewithPath + ".jasper", param, con);

			// generate a new name for file
			SecureRandom random = new SecureRandom();
			String unique_ext = "_" + new BigInteger(30, random).toString(32) + "." + type;
			String resultFileNameWithPath = fileNamewithPath + unique_ext;
			switch (type) {
			case "pdf":
				JasperExportManager.exportReportToPdfFile(jasperPrint, resultFileNameWithPath);
				break;
			case "html":
				JasperExportManager.exportReportToHtmlFile(jasperPrint, resultFileNameWithPath);
				break;
			case "xls":
				JRXlsExporter exporterXLS = new JRXlsExporter();
				exporterXLS.setParameter(JRXlsExporterParameter.JASPER_PRINT, jasperPrint);
				exporterXLS.setParameter(JRExporterParameter.INPUT_FILE_NAME, fileNamewithPath + ".jasper");
				exporterXLS.setParameter(JRExporterParameter.OUTPUT_FILE_NAME, resultFileNameWithPath);
				exporterXLS.setParameter(JRXlsExporterParameter.IS_DETECT_CELL_TYPE, Boolean.TRUE);
				exporterXLS.setParameter(JRXlsExporterParameter.IS_WHITE_PAGE_BACKGROUND, Boolean.FALSE);
				exporterXLS.setParameter(JRXlsExporterParameter.IS_REMOVE_EMPTY_SPACE_BETWEEN_ROWS, Boolean.TRUE);
				exporterXLS.setParameter(JRXlsExporterParameter.IS_REMOVE_EMPTY_SPACE_BETWEEN_COLUMNS, Boolean.TRUE);
				exporterXLS.setParameter(JRXlsExporterParameter.IS_COLLAPSE_ROW_SPAN, Boolean.TRUE);
				exporterXLS.setParameter(JRXlsExporterParameter.IS_IGNORE_GRAPHICS, Boolean.FALSE);
				exporterXLS.exportReport();
				break;
			case "view":
				// output the html string
				type = "html";
				JasperExportManager.exportReportToHtmlFile(jasperPrint, resultFileNameWithPath);
				Scanner scanner;
				try {
					scanner = new Scanner(new File(resultFileNameWithPath));
					String html = scanner.useDelimiter("\\A").next();
					scanner.close();
					File f = new File(resultFileNameWithPath);
					f.delete();
					resultFileNameWithPath = html;
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
				break;
			default:
				type = "html";
				JasperExportManager.exportReportToHtmlFile(jasperPrint, resultFileNameWithPath);
			}
			return resultFileNameWithPath;
		} catch (JRException ex) {
			System.out.println(ex.getMessage());
			/* nu am raport jasper ... deci e posibil sa fie un raport grafic */
			/* trebuie sa creez parametrii din ceea ce exista in param */
			// throw new DBException("NO FILE");
			return "No File";
		}
	}
	
	
	/**
	 * Generate an Excel file with the SQL result
	 * 
	 * @param p_strSQL
	 * @return full path and file name
	 */
	public String SQLToExcel(String p_strSQL, String p_fileName) {

		// get the Table
		DBTable T = new DBTable();
		DbManager.getDB().getDBTable(null, T, p_strSQL);

		return DBTableToExcel(T, p_fileName);
	} // SQLToExcel

	/**
	 * Generate an Excel file with the content of the DBTable
	 * 
	 * @param T
	 * @param p_fileName
	 * @return full path and file name
	 */
	public String DBTableToExcel(DBTable T, String p_fileName) {

		// generate the XLS file
		// file names
		String path = DBConnection.FilesRepository;
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

		String ext = "xlsx";
		String fileName = p_fileName + "_" + String.format("%s.%s", RandomStringUtils.randomAlphanumeric(8), ext);

		String fileRealName = path + "/" + fileName;

		// Blank workbook
		XSSFWorkbook workbook = new XSSFWorkbook();

		XSSFCellStyle date_style = workbook.createCellStyle();
		date_style.setDataFormat((short) BuiltinFormats.getBuiltinFormat("m/d/yy"));
		XSSFCellStyle numeric_style = workbook.createCellStyle();
		numeric_style.setDataFormat((short) BuiltinFormats.getBuiltinFormat("# ##0.00"));

		// Create a blank sheet
		XSSFSheet sheet = workbook.createSheet("Sheet1");

		String strTemp = "";

		// header row
		XSSFRow headerRow = sheet.createRow(0);
		for (int j = 0; j < T.Fields.size(); j++) {
			XSSFCell cell = headerRow.createCell(j);
			cell.setCellValue((String) T.Fields.get(j));
		}

		// for all columns in the table
		for (int i = 0; i < T.reccount(); i++) {

			XSSFRow normalRow = sheet.createRow(i + 1);

			try {

				for (int j = 0; j < T.Fields.size(); j++) {

					XSSFCell cell = normalRow.createCell(j);

					try {
						strTemp = T.get(i).get(T.Fields.get(j)).toString();
					} catch (Exception e) {
						strTemp = "";
					}
					/*
					 * if (NumberUtils.isNumber(strTemp)) {
					 * cell.setCellValue(Double.parseDouble(strTemp));
					 * cell.setCellStyle(numeric_style); } else {
					 */
					switch (T.FieldTypes.get(j)) {
					case "DATE":
						if (strTemp != null) {
							if (!strTemp.isEmpty()) {
								cell.setCellValue(df.parse(strTemp));
								cell.setCellStyle(date_style);
							}
						}
						break;

					case "NUMERIC":
					case "DOUBLE":
						// pe aici nu intra deoarece numeric e pe if
						cell.setCellValue(Double.parseDouble(strTemp));
						cell.setCellStyle(numeric_style);
						break;

					default:
						cell.setCellValue(strTemp);
					}

				}

			} catch (Exception e) {
				System.out.println(e.toString());
			}
		}

		// evaluate all formulas
		XSSFFormulaEvaluator.evaluateAllFormulaCells(workbook);

		try {
			// Write the workbook in file system
			FileOutputStream out = new FileOutputStream(new File(fileRealName));
			workbook.write(out);
			out.close();
			workbook.close();
			// System.out.println(fileRealName +
			// " written successfully on disk.");
		} catch (Exception e) {
			e.printStackTrace();
		}

		return fileRealName;

	} // DBTableToExcel

	/**
	 * Generate an Excel file with the SQL result using a template
	 * 
	 * @param p_strSQL
	 * @param p_templateName
	 *           - template name
	 * @param p_sheetName
	 *           - the name of the sheet where to write the data
	 * @return full path and file name
	 */
	public String SQLToTemplateExcel(String p_strSQL, String p_fileName, String p_templateName, String p_sheetName) {

		// get the Table
		DBTable T = new DBTable();
		DbManager.getDB().getDBTable(null, T, p_strSQL);
		return DBTableToTemplateExcel(T, p_fileName, p_templateName, p_sheetName);
	} // SQLToTemplateExcel

	/**
	 * Generate an Excel file with the content of the DBTable using a template
	 * 
	 * @param T
	 * @param p_fileName
	 * @param p_templateName
	 *           - template name
	 * @param p_sheetName
	 *           - the name of the sheet where to write the data
	 * @return full path and file name
	 */
	public String DBTableToTemplateExcel(DBTable T, String p_fileName, String p_templateName, String p_sheetName) {

		// generate the XLS file
		String path = DBConnection.FilesRepository;
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

		String ext = "xls";
		// random file name
		String fileName = p_fileName + "_" + String.format("%s.%s", RandomStringUtils.randomAlphanumeric(8), ext);
		String fileRealName = path + "/" + fileName;
		String templateName = p_templateName + "." + ext;
		String templateRealName = path + "/" + templateName;

		// generate a new file based by template

		try {

			// copy template to real file
			File source = new File(templateRealName);
			File dest = new File(fileRealName);
			FileUtils.copyFile(source, dest);

			// open the file
			Workbook workbook = WorkbookFactory.create(source);
			Sheet sheet = workbook.getSheet(p_sheetName);

			org.apache.poi.ss.usermodel.CellStyle date_style = workbook.createCellStyle();
			date_style.setDataFormat((short) BuiltinFormats.getBuiltinFormat("m/d/yy"));
			org.apache.poi.ss.usermodel.CellStyle numeric_style = workbook.createCellStyle();
			numeric_style.setDataFormat((short) BuiltinFormats.getBuiltinFormat("# ##0.00"));

			String strTemp = "";

			// header row
			org.apache.poi.ss.usermodel.Row headerRow = sheet.createRow(0);
			for (int j = 0; j < T.Fields.size(); j++)

			{
				org.apache.poi.ss.usermodel.Cell cell = headerRow.createCell(j);
				cell.setCellValue((String) T.Fields.get(j));
			}

			// for all columns in the table
			for (

					int i = 0; i < T.reccount(); i++)

			{

				org.apache.poi.ss.usermodel.Row normalRow = sheet.createRow(i + 1);

				try {

					for (int j = 0; j < T.Fields.size(); j++) {

						org.apache.poi.ss.usermodel.Cell cell = normalRow.createCell(j);

						try {
							strTemp = T.get(i).get(T.Fields.get(j)).toString();
						} catch (Exception e) {
							strTemp = "";
						}
						/*
						 * if (NumberUtils.isNumber(strTemp)) {
						 * cell.setCellValue(Double.parseDouble(strTemp)); } else {
						 * cell.setCellValue(strTemp); }
						 */
						switch (T.FieldTypes.get(j)) {
						case "DATE":
							if (strTemp != null) {
								if (!strTemp.isEmpty()) {
									cell.setCellValue(df.parse(strTemp));
									cell.setCellStyle(date_style);
								}
							}
							break;

						case "NUMERIC":
						case "DOUBLE":
							// pe aici nu intra deoarece numeric e pe if
							cell.setCellValue(Double.parseDouble(strTemp));
							cell.setCellStyle(numeric_style);
							break;

						default:
							cell.setCellValue(strTemp);
						}
					}

				} catch (Exception e) {
					System.out.println(e.toString());
				}
			}

			HSSFFormulaEvaluator.evaluateAllFormulaCells(workbook);

			// Write the workbook in file system
			FileOutputStream out = new FileOutputStream(new File(fileRealName));
			workbook.write(out);
			out.flush();
			out.close();
			workbook.close();

		} catch (Exception e) {
			// the ERROR !!!
			e.printStackTrace();
		}

		return fileRealName;

	} // DBTableToTemplateExcel

	/**
	 * Generate an Excel file with the SQL result using a template (multiple
	 * sheets)
	 * 
	 * @param LT
	 *           - list of tables
	 * @param p_fileName
	 * @param p_templateName
	 *           - template name
	 * @param p_LsheetName
	 *           - list of sheet names - the name of the sheet where to write the
	 *           data
	 * @return full path and file name
	 */
	public String SQLToTemplateExcel(String p_strSQL, String p_fileName, String p_templateName, List<String> p_LsheetName) {
		// get the Table
		List<DBTable> LT = new ArrayList<DBTable>();
		// call the sql - with multiple results
		DbManager.getDB().getDBXTable(null, LT, p_strSQL);
		return DBTableToTemplateExcel(LT, p_fileName, p_templateName, p_LsheetName);

	} // SQLToTemplateExcel

	/**
	 * Generate an Excel file with the content of the DBTable using a template
	 * (multiple sheets)
	 * 
	 * @param LT
	 *           - list of tables
	 * @param p_fileName
	 * @param p_templateName
	 *           - template name
	 * @param p_LsheetName
	 *           - list of sheet names - the name of the sheet where to write the
	 *           data
	 * @return full path and file name
	 */
	public String DBTableToTemplateExcel(List<DBTable> LT, String p_fileName, String p_templateName, List<String> p_LsheetName) {

		// generate the XLS file
		String path = DBConnection.FilesRepository;
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

		String ext = "xls";
		// random file name
		String fileName = p_fileName + "_" + String.format("%s.%s", RandomStringUtils.randomAlphanumeric(8), ext);
		String fileRealName = path + "/" + fileName;
		String templateName = p_templateName + "." + ext;
		String templateRealName = path + "/" + templateName;

		// generate a new file based by template

		try {

			// copy template to real file
			File source = new File(templateRealName);
			File dest = new File(fileRealName);
			FileUtils.copyFile(source, dest);

			// open the file
			Workbook workbook = WorkbookFactory.create(source);

			org.apache.poi.ss.usermodel.CellStyle date_style = workbook.createCellStyle();
			date_style.setDataFormat((short) BuiltinFormats.getBuiltinFormat("m/d/yy"));
			org.apache.poi.ss.usermodel.CellStyle numeric_style = workbook.createCellStyle();
			numeric_style.setDataFormat((short) BuiltinFormats.getBuiltinFormat("# ##0.00"));

			// for each sheet
			String sheetName;
			DBTable T;
			Sheet sheet;
			String strTemp;
			org.apache.poi.ss.usermodel.Row headerRow;

			for (int list_i = 0; list_i < p_LsheetName.size(); list_i++) {

				sheetName = p_LsheetName.get(list_i);
				T = LT.get(list_i);

				sheet = workbook.getSheet(sheetName);

				// System.out.println(sheetName);
				// System.out.println(sheet);

				strTemp = "";

				// header row
				headerRow = sheet.createRow(0);
				for (int j = 0; j < T.Fields.size(); j++)

				{
					org.apache.poi.ss.usermodel.Cell cell = headerRow.createCell(j);
					cell.setCellValue((String) T.Fields.get(j));
					// System.out.println(j + " " + (String) T.Fields.get(j));
				}

				// for all columns in the table
				for (int i = 0; i < T.reccount(); i++) {

					org.apache.poi.ss.usermodel.Row normalRow = sheet.createRow(i + 1);

					try {

						for (int j = 0; j < T.Fields.size(); j++) {

							org.apache.poi.ss.usermodel.Cell cell = normalRow.createCell(j);

							try {
								strTemp = T.get(i).get(T.Fields.get(j)).toString();
							} catch (Exception e) {
								strTemp = "";
							}
							/*
							 * if (NumberUtils.isNumber(strTemp)) {
							 * cell.setCellValue(Double.parseDouble(strTemp)); } else {
							 * cell.setCellValue(strTemp); }
							 */
							try {
								switch (T.FieldTypes.get(j)) {
								case "DATE":
									if (strTemp != null) {
										if (!strTemp.isEmpty()) {
											cell.setCellValue(df.parse(strTemp));
											cell.setCellStyle(date_style);
										}
									}
									break;

								case "NUMERIC":
								case "DOUBLE":
									try {
										// pe aici nu intra deoarece numeric e pe if
										cell.setCellValue(Double.parseDouble(strTemp));
										cell.setCellStyle(numeric_style);
									} catch (Exception e) {
										/* fortez sa fie string */
										cell.setCellValue(strTemp);
									}
									break;

								default:
									cell.setCellValue(strTemp);
								}
							} catch (Exception e) {
								System.out.println("Name" + T.Fields.get(j));
								System.out.println("Value: " + strTemp);
								System.out.println("Type: " + T.FieldTypes.get(j));
								System.out.println(e.toString());
							}

						}

					} catch (Exception e) {
						System.out.println(e.toString());
					}
				}
			}

			// evaluate all formulas
			HSSFFormulaEvaluator.evaluateAllFormulaCells(workbook);

			// Write the workbook in file system
			FileOutputStream out = new FileOutputStream(new File(fileRealName));
			workbook.write(out);
			out.flush();
			out.close();
			workbook.close();

		} catch (Exception e) {
			// the ERROR !!!
			e.printStackTrace();
		}

		return fileRealName;

	} // DBTableToTemplateExcel

	/**
	 * PrintFile
	 * 
	 * @param p_cFileNameWithPath
	 * @return error message or empty string
	 */
	public String PrintFile(String p_cFileNameWithPath) {
		Desktop desktop = Desktop.getDesktop();
		try {
			desktop.print(new File(p_cFileNameWithPath));
			return "";
		} catch (IOException e) {
			e.printStackTrace();
			return e.getMessage();
		}
	}

	/**
	 * PrintPDFFile (usinf Apache PDFBox
	 * 
	 * @param p_cFileNameWithPath
	 * @return error message or empty string
	 */
	public String PrintPDFFile(String p_cFileNameWithPath) {
		PDDocument document;
		String eMessage = "";
		try {
			document = PDDocument.load(new File(p_cFileNameWithPath));

			// choose your printing method:
			eMessage += "0";
			PrinterJob job = PrinterJob.getPrinterJob();
			eMessage += "1";
			job.setPageable(new PDFPageable(document));
			eMessage += "2";
			job.print();

			return "";

		} catch (Exception e) {
			return eMessage + e.getMessage();
		}

	}

}
