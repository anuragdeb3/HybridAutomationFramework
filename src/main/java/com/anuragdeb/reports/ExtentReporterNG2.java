package com.anuragdeb.reports;

import java.io.File;
import java.util.Calendar;
import java.util.Date;

import com.anuragdeb.helper.Library;
import com.aventstack.extentreports.ExtentReports;
import com.aventstack.extentreports.ExtentTest;
import com.aventstack.extentreports.reporter.ExtentSparkReporter;

public class ExtentReporterNG2 {
	
public static ExtentReports extent;
public static ExtentSparkReporter report;
public static ExtentTest test;


	public static void ReportSuite() {
		
		report = new ExtentSparkReporter(System.getProperty("user.dir")+"//Reports"+File.separator+"ExtentReport.html");
		report.config().setDocumentTitle("Extent Report");
		report.config().setReportName("Sanity Test Cases");
		
		
		extent = new ExtentReports();
		extent.attachReporter(report);
		
		extent.setSystemInfo("Company Name", Library.getVal("CompanyName"));
		extent.setSystemInfo("Project Name", Library.getVal("ProjectName"));
		extent.setSystemInfo("Tester Name", Library.getVal("TesterName"));

	//	String testMethodName = result.getClass().getCanonicalName();
		
		//test = extent.createTest(testMethodName);
		
	
	}
	
	
	public void ReportEnd() {
		extent.flush();
	}

	private Date getTime(long millis) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(millis);
		return calendar.getTime();
	}
}
