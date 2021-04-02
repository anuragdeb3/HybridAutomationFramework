package com.anuragdeb.helper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.Assert;
import org.testng.asserts.SoftAssert;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.SftpProgressMonitor;

public class Library {

	public static WebDriver driver;

	public Library(WebDriver driver) {

		Library.driver = driver;
	}

	// final static Logger log = Logger.getLogger(Library.class);
	SoftAssert softAssert = new SoftAssert();
	public static Properties prop = new Properties();

	public String getClassName() {

		Class<?> enclosingClass = getClass().getEnclosingClass();
		if (enclosingClass != null) {
			return enclosingClass.getName();
		} else {
			return getClass().getName();
		}

	}

	public static String getVal(String key) {
		String value = prop.getProperty(key);
		return value;
	}

	public static void LoadConfig(String FileName) {
		String path = System.getProperty("user.dir");
		String fpath = path + "\\" + FileName;
		try {
			FileInputStream inStream = new FileInputStream(fpath);
			prop.load(inStream);
			inStream.close();
		} catch (NullPointerException e) {
			System.out.println("Can't load null, Check Path. Cause : " + e);
			try {
				
				System.out.println("Kindly Check if the parameters are blank or not :" + e);
			} catch (Exception e1) {
				
				e1.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			System.out.println("Not Able to Load File, Check Path. Cause : " + e);
			try {
				

				System.out.println("Cause : " + e
						+ "File Not Found Exception Raised, Kindly Check if the File path is correct or not");
			} catch (Exception e1) {
				

				e1.printStackTrace();
			}
		} catch (IOException e) {
			System.out.println("IOException, Check Path. Cause : " + e);
		} catch (Exception e) {
			System.out.println("Exception raised, Check Path. Cause : " + e);
			try {
				

				System.out.println("Kindly Check LoadConfig method");
			} catch (Exception e1) {
				
				e1.printStackTrace();
			}

		}

	}

	public static void reLoadingPage(WebDriver driver) throws Exception {
		driver.navigate().refresh();
		driver.navigate().refresh();
		driver.manage().timeouts().pageLoadTimeout(5000, TimeUnit.SECONDS);
	}

	public static boolean IsElementPresent(WebDriver driver, String locator, String locatorValue) {
		try {
			WebElement element = null ;

			if(locator.equalsIgnoreCase("xpath")){
					element = driver.findElement(By.xpath(locatorValue));
					}
			else if(locator.equalsIgnoreCase("id")){
				element = driver.findElement(By.id(locatorValue));
				}
			else if(locator.equalsIgnoreCase("className")){
				element = driver.findElement(By.className(locatorValue));
				}
			else if(locator.equalsIgnoreCase("tagName")){
				element = driver.findElement(By.tagName(locatorValue));
				}

			if (element.isDisplayed() == true) {
				// System.out.println("Element Present");

			}
			return true;
		} catch (Exception e) {
			// System.out.println("Exception occured Element Not found occured"+e);
			return false;

		}

	}

	public static boolean Ispresent(WebDriver driver, WebElement ele) {
		try {

			if (ele.isDisplayed() == true) {
				// System.out.println("Element Present");

			}
			return true;
		} catch (Exception e) {
			// System.out.println("Exception occured Element Not found occured"+e);
			return false;

		}

	}

	public static String GetAnyZoneCurrentDate(String Timezone, String DateFormat) {
		String CurrentDate;
		SimpleDateFormat format = new SimpleDateFormat(DateFormat);
		format.setTimeZone(TimeZone.getTimeZone(Timezone));
		CurrentDate = format.format(new Date());
		return CurrentDate;
	}

	/**
	 * This Method determines if the string is numeric or not even if it is of
	 * decimal
	 * 
	 * @param strNum
	 *            : String Value that has to be determined
	 */

	public static boolean isNumeric(String strNum) {
		if (strNum == null) {
			return false;
		}
		try {
			double d = Double.parseDouble(strNum);
		} catch (NumberFormatException nfe) {
			return false;
		}
		return true;
	}

	/**
	 * This Method Copy the file from source to destination folder
	 * 
	 * @param from
	 *            : Source path with filename
	 * @param to
	 *            : Destination path
	 */
	public static void copyFile(String source, String destination) {
		Path src = Paths.get(source);
		Path dest = Paths.get(destination);
		try {
			Files.copy(src, dest.resolve(src.getFileName()));
			System.out.println("File Copied Successfully");
		} catch (IOException e) {
			System.out.println("File Copying Failed...");
			e.printStackTrace();
		}
	}

	public static void DeleteFile(String source) {
		Path src = Paths.get(source);
		try {
			if (Files.exists(src)) {
				Files.delete(src);
				System.out.println("File Deleted Successfully");
			}

		} catch (Exception e) {
			System.out.println("File Deletion Failed..." + e);

		}
	}

	/**
	 * 
	 * @return This method return the Date and Time in yyyy-MM-dd Format
	 */
	public static String getCurrentDate() {
		Date d = new Date();
		String mydate = new SimpleDateFormat("yyyy-MM-dd").format(d);
		return mydate;
	}

	/**
	 * 
	 * @param DateFormat
	 *            : Date in YYYYMMdd format
	 * @return : this method will return the current system date in YYYYMMdd format
	 */
	public static String getCustomisedDate(String DateFormat) {
		Date d = new Date();
		String mydate = new SimpleDateFormat(DateFormat).format(d);
		// example : yyyy-MM-dd

		return mydate;
	}

	// Change any date format to desired pattern

	public static String getCustomisedExistDate(String Date, String CurrDateFormat, String ReqDateFormat) {

		String reqDAte = "";
		try {
			if (Date == null) {
				System.out.println("Date is NULL Check SQL Query used");

			}

			SimpleDateFormat dt = new SimpleDateFormat(CurrDateFormat);
			Date date = dt.parse(Date);

			SimpleDateFormat dt1 = new SimpleDateFormat(ReqDateFormat);
			reqDAte = dt1.format(date);
			System.out.println(reqDAte);

		} catch (ParseException p) {
			System.out.println("Parse Exception raised at getCustomisedExistDate..." + p);
		} catch (Exception e) {
			System.out.println("Generic Exception raised at getCustomisedExistDate..." + e);
			Assert.fail();
		}

		return reqDAte;
	}

	public static List<String> getListRunCmdOnLinux(String command, String host, String user, String password) {
		/*
		 * String host = Library.getVal("host"); String user = Library.getVal("user");
		 * String password = Library.getVal("password");
		 */
		List<String> lineList = new ArrayList<String>();
		JSch jsch = new JSch();
		com.jcraft.jsch.Session session;
		try {
			session = jsch.getSession(user, host, 22);
			session.setPassword(password);
			session.setConfig("StrictHostKeyChecking", "no");
			session.setConfig("PreferredAuthentications", "publickey,keyboard-interactive,password");
			session.connect();
			System.out.println("Connected");

			Channel channel = session.openChannel("exec");
			((ChannelExec) channel).setCommand(command);
			channel.setInputStream(null);
			((ChannelExec) channel).setErrStream(System.err);

			InputStream in = channel.getInputStream();
			channel.connect();

			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String line;
			int index = 0;

			while ((line = reader.readLine()) != null) {
				// System.out.println(++index + " : " + line);
				lineList.add(line);
			}

			System.out.println("Connection established.");
			System.out.println("Creating SFTP Channel.");

			channel.disconnect();
			session.disconnect();
		} catch (Exception e) {
			System.out.println("Generic Exception raised at getListRunCmdOnLinux method" + e);
			// e.printStackTrace();
		}
		return lineList;
	}

	public static BigDecimal truncateDecimal(double value, int numberofDecimals) {
		if (value > 0) {
			return new BigDecimal(String.valueOf(value)).setScale(numberofDecimals, BigDecimal.ROUND_FLOOR);
		} else {
			return new BigDecimal(String.valueOf(value)).setScale(numberofDecimals, BigDecimal.ROUND_CEILING);
		}
	}

	/**
	 * 
	 * 
	 * @param KEY
	 *            : Key from the property File
	 * @param CONFIG_PATH
	 *            : Property file path
	 * @return This method will return the value of the key passed form property
	 *         file
	 */

	public static void Sleep(long TimeInMiliSeconds) {
		try {
			Thread.sleep(TimeInMiliSeconds);
		} catch (InterruptedException e1) {
			
			e1.printStackTrace();
		}
	}

	/**
	 * 
	 * @param command
	 *            command with complete path
	 * @param host
	 *            Name of the Host
	 * @param user
	 *            User name to login
	 * @param password
	 *            password to login
	 * @return This method run any shell script or any command given in linux system
	 *         through putty.
	 */
	@SuppressWarnings("deprecation")
	public static void run_SparkJobOnLinux(String command, String host, String user, String password) {
		// String consoleMessage=null;
		JSch jsch = new JSch();
		Session session;
		try {
			session = jsch.getSession(user, host, 22);
			session.setPassword(password);
			session.setConfig("StrictHostKeyChecking", "no");
			session.setConfig("PreferredAuthentications", "publickey,keyboard-interactive,password");
			session.connect();
			System.out.println("Connected to " + host);
		
			Channel channel = session.openChannel("exec");
			((ChannelExec) channel).setCommand(command);
			channel.setInputStream(null);
			((ChannelExec) channel).setErrStream(System.err);

			InputStream in = channel.getInputStream();
			channel.connect();

			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String line;
			List<String> consoleMsgList = new ArrayList<String>();
			int index = 0;
			while ((line = reader.readLine()) != null) {
				System.out.println(++index + " : " + line);
				consoleMsgList.add(line);

			}

			boolean flag = false;
			for (String Msgline : consoleMsgList) {

				if (Msgline.contains("Spark Job Successfully completed")) {
					System.out.println("Spark Job Ran Successfully");
					
					flag = true;
				}
			}

			if (flag == false) {

				System.out.println("Spark Job failed");
				
				Assert.assertTrue(flag == false, "Spark Job failed");
			}

			channel.disconnect();
			session.disconnect();
		} catch (Exception e) {
			try {

				System.out.println("Generic Exception raised at run_SparkJobOnLinux" + e);
				
			} catch (Exception e1) {
				
				e1.printStackTrace();
			}
			e.printStackTrace();
		}
	}

	public static void getExcel_OnLinux(String host, String user, String password, String readFilePath,
			String localPath) {

		JSch jsch = new JSch();
		Session session;
		try {
			session = jsch.getSession(user, host, 22);
			session.setPassword(password);
			session.setConfig("StrictHostKeyChecking", "no");
			session.setConfig("PreferredAuthentications", "publickey,keyboard-interactive,password");
			session.connect();
			System.out.println("Connected to Host " + host);
			
			Channel channel = session.openChannel("sftp");
			channel.connect();

			ChannelSftp sftpChannel = (ChannelSftp) channel;

			sftpChannel.get(readFilePath, localPath);

			channel.disconnect();
			session.disconnect();
		} catch (Exception e) {
			try {
				
				System.out.println(e.getMessage() + "Get Excel from Linux Server, Not able to fetch Excel File.");

			} catch (Exception e1) {
				
				e1.printStackTrace();
			}
			e.printStackTrace();
		}
	}

	public static void WaitForPageLoad(WebDriver driver, int timeOut) {
		System.out.println("Wait for Page Load Started. Duration :" + timeOut);
		new WebDriverWait(driver, timeOut).until(webDriver -> ((JavascriptExecutor) webDriver)
				.executeScript("return document.readyState").equals("complete"));
	}

	public static void waitForGivenTimeInSeconds(int wTmeInSeconds) {
		try {
			TimeUnit.SECONDS.sleep(wTmeInSeconds);
		} catch (InterruptedException waitException) {
			waitException.printStackTrace();
		}
	}

	// Methods for webelemt waits
	public static void waitUntilElementIsVisible(WebDriver driver, long ITO, WebElement WebElement) {
		WebDriverWait wait = new WebDriverWait(driver, ITO);
		wait.until(ExpectedConditions.visibilityOf(WebElement));
	}

	public static void waitUntilelementToBeClickable(WebDriver driver, long ITO, WebElement WebElement) {
		try {
			WebDriverWait wait = new WebDriverWait(driver, ITO);
			wait.until(ExpectedConditions.elementToBeClickable(WebElement));
		} catch (Exception e) {

			if (e.getMessage().contains("Expected condition failed: waiting for element to be clickable")) {

				try {
					
					System.out.println(e.getMessage()
							+ "Performance Issues, the reason might be due to performance issue/network issue, the loading is not completed ");

				} catch (Exception e1) {
					
					e1.printStackTrace();
				}

			}

		}
	}

	public static void waitUntilelementToBeSelected(WebDriver driver, long ITO, WebElement WebElement) {
		WebDriverWait wait = new WebDriverWait(driver, ITO);
		wait.until(ExpectedConditions.elementToBeSelected(WebElement));
	}

	public static void waitUntiltitleContains(WebDriver driver, long ITO, String WebElement) {
		WebDriverWait wait = new WebDriverWait(driver, ITO);
		wait.until(ExpectedConditions.titleContains(WebElement));
	}

	public static void waitUntiltitleIs(WebDriver driver, long ITO, String eTitle) {
		WebDriverWait wait = new WebDriverWait(driver, ITO);
		wait.until(ExpectedConditions.titleIs(eTitle));
	}

	public static void ScrollTo(WebDriver driver, WebElement element) {

		((JavascriptExecutor) driver).executeScript("arguments[0].scrollIntoView(true);", element);
	}

	public static void scrolldown(WebDriver driver, int ScrollBy) {
		JavascriptExecutor js = ((JavascriptExecutor) driver);
		js.executeScript("window.scrollBy(0," + ScrollBy + ")");
		Library.waitForGivenTimeInSeconds(2);

	}

	public static void scrollUp(WebDriver driver) {
		JavascriptExecutor js = ((JavascriptExecutor) driver);
		js.executeScript("window.scrollBy(0,0)");
		System.out.println("Scrolled up");
		Library.waitForGivenTimeInSeconds(2);

	}

	public static String getTextValue(WebElement element) {
		String value = "";
		try {

			value = element.getText();

		} catch (Exception genericE) {
			genericE.printStackTrace();
		}
		return value;
	}

	/*
	 * FileCopierOverNetwork Copy Files between Linux & Windows using Java
	 * 
	 */

	final static SftpProgressMonitor monitor = new SftpProgressMonitor() {
		public void init(final int op, final String source, final String target, final long max) {
			System.out.println("sftp start uploading file from:" + source + " to:" + target);
		}

		public boolean count(final long count) {

			// log.debug("sftp sending bytes: "+count);
			System.out.println("sftp sending bytes: " + count);
			return true;
		}

		public void end() {
			System.out.println("sftp uploading is done.");
			System.out.println("sftp uploading is done.");
		}
	};

	/*
	 * Will copy file from one server to another Below method will upload the data
	 * from Window to Linux Server using put()
	 */

	public static void putFile(String hostname, String username, String password, String copyFrom, String copyTo) {
		System.out.println("Initiate getting file from Linux Server...");
		Session session = null;
		ChannelSftp sftpChannel = null;
		String excelPath = copyFrom + Library.getVal("ExcelName");
		System.out.println(excelPath);

		try {
			
			// from Linux Server...");
			System.out.println("Connect to Server, Initiate getting file from Linux Server...");
			JSch jsch = new JSch();

			System.out.println("Trying to connect.....");
		
			// connect.....");
			session = jsch.getSession(username, hostname, 22);
			session.setConfig("StrictHostKeyChecking", "no");
			session.setPassword(password);
			session.connect();
			System.out.println("is server connected? " + session.isConnected());
			
			Channel channel = session.openChannel("sftp");
			channel.connect();
			sftpChannel = (ChannelSftp) channel;

			System.out.println(sftpChannel.getHome());
		} catch (SftpException | JSchException e) {

			System.out.println("Exception is raised in putFile method :" + e);

			try {
				
				System.out.println("Exception Raised, Kindly Check putFile method ");
			} catch (Exception e1) {
				
				e1.printStackTrace();
			}

			e.printStackTrace();
		}
		try {
			sftpChannel.put(excelPath, copyTo, monitor, ChannelSftp.OVERWRITE);
		} catch (SftpException e) {
			System.out.println("SftpException: " + copyFrom);
			e.printStackTrace();
		} catch (Exception e1) {
			System.out.println("File status : file was not found: " + copyFrom);
		} finally {

			sftpChannel.exit();
			session.disconnect();
			System.out.println("File Load" + " Finished getting file from Linux Server...");
			
		}
	}

	/*
	 * Will copy file from one server to another Below method will download the data
	 * from Linux Server to Windows using get()
	 */

	public static void getFile(String hostname, String username, String password, String copyFrom, String copyTo)
			throws Exception {
		System.out.println("Initiate getting file from Linux Server...");
		Session session = null;
		ChannelSftp sftpChannel = null;
		String excelPath = copyTo + Library.getVal("ExcelName");
		System.out.println(excelPath);
		System.out.println("Excel Path" + excelPath);
		try {
			

			System.out.println("Get File from Server, Initiate getting file from Linux Server...");
			JSch jsch = new JSch();

			System.out.println("Trying to connect.....");
		
			session = jsch.getSession(username, hostname, 22);
			session.setConfig("StrictHostKeyChecking", "no");
			session.setPassword(password);
			session.connect();
			System.out.println("is server connected? " + session.isConnected());
		
			Channel channel = session.openChannel("sftp");
			channel.connect();
			sftpChannel = (ChannelSftp) channel;

			System.out.println(sftpChannel.getHome());
		} catch (SftpException e) {
			System.out.println("Exception raised at getFile" + e);

		}
		try {
			sftpChannel.get(copyFrom, excelPath, monitor, ChannelSftp.OVERWRITE);
		} catch (SftpException e) {
			// System.out.println("SftpException: " + copyFrom);
			System.out.println("SftpException is raised in getFile method :" + e);

			Assert.fail();

		} finally {

			sftpChannel.exit();
			session.disconnect();
			System.out.println("Finished getting file from Linux Server...");
			System.out.println("Session disconnected, Finished getting file from Linux Server...");

		}

	}

	public static File createShellFile(String fileName) {
		File file = new File(fileName + ".sh");
		if (file.exists()) {
			return file;
		}
		try {
			FileWriter fileWriter = new FileWriter(file);
			fileWriter.write("#!/bin/sh");
			fileWriter.write("\n");
			fileWriter.write("echo \"Test has started\"");
			fileWriter.write("\n");
			fileWriter.write("Anurag Deb");
			fileWriter.flush();
			fileWriter.close();
			return file;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public List<String> getlistfromdriver(List<WebElement> list) {

		List<String> newlist = new ArrayList<String>();
		int count = 0;
		if (list.size() > 0) {

			for (count = 0; count < list.size(); count++) {
				if ((list.get(count).getText() != null) && !(list.get(count).getText().isEmpty())) {
					newlist.add(list.get(count).getText());
				}
			}

		}

		return newlist;
	}

	public static List<String> WebEleToStringList(List<WebElement> WebElelist) {

		List<String> valuelist = new ArrayList<String>();
		for (WebElement item : WebElelist) {
			valuelist.add(item.getText());

		}

		return valuelist;

	}

	public static void zoomInZoomOut(WebDriver driver, int value) {
		JavascriptExecutor js = (JavascriptExecutor) driver;
		System.out.println(value);
		js.executeScript("document.body.style.zoom='" + value + "%'");
	}

	public static void deleteFile(String path) {

		try {
			Files.deleteIfExists(Paths.get(path));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static boolean matchList(List<?> list1, List<?> list2) {

		boolean flag = true;
		try {
			if (list1.size() == list2.size()) {
				System.out.println("Size is Same");


				if (list1.equals(list2)) {
					System.out.println("List Compare,  Both list are equal");
					
					flag = true;
				} else {
					System.out
							.println("List Compare:  Both list are not equal List1 :" + list1 + " \n list2: " + list2);
					
					flag = false;
				}

			} else {
				System.out.println("List Size:  Both list have different length.\n List1 :" + list1.size() + "\n List2:"
						+ list2.size());
				flag = false;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return flag;
	}

	public static boolean isNullOrEmptyMap(Map<?, ?> map) {
		return (map == null || map.isEmpty());
	}

	/**
	 * @author Anurag_Deb This method will check the difference of Key's Value in
	 *         two maps
	 * @param map1
	 * @param map2
	 * @return result in boolean format
	 * @throws Exception
	 */

	public static boolean checkDiffMap(Map<String, ?> map1, Map<String, ?> map2) throws Exception {

		if (isNullOrEmptyMap(map1) || isNullOrEmptyMap(map2)) {
			System.out.println("One of the Map is Empty. Map1.size :" + map1.size() + " Map2.size :" + map2.size());
			System.out.println("List Compare : One of the list is empty.  Map1.size :" + map1.size() + " Map2.size :"
					+ map2.size());

			return false;
		} else {

			boolean SizeCheck = map1.size() == map2.size();
			System.out.println("Verify Size of Maps. Status : " + SizeCheck + "   Map1 Size : " + map1.size()
					+ "   Map2 Size : " + map2.size());
			System.out.println("List Compare:  Verify Size of Maps. Status : " + SizeCheck + "Map1 Size : "
					+ map1.size() + "Map2 Size : " + map2.size());

			for (Map.Entry<String, ?> me : map2.entrySet()) {
				String key = me.getKey();
				if (map1.containsKey(key)) {

					if (map2.get(key).equals(map1.get(key))) {
						System.out.println("Map1 Value :" + map1.get(key) + " =  Map2 Value :" + map2.get(key)
								+ " for Key :" + key);

					} else {
						System.out.println("Map1 Value :" + map1.get(key) + " !=  Map2 Value :" + map2.get(key)
								+ " for Key :" + key);
						System.out.println("List Compare: Map1 Value :" + map1.get(key) + " !=  Map2 Value :"
								+ map2.get(key) + " for Key :" + key);

						return false;
					}
				}
			}
		}
		return true;

	} // End of checkDiffMap Keyword

	public static void listALLFolders(String path) {
		try (Stream<Path> walk = Files.walk(Paths.get(path))) {

			List<String> result = walk.filter(Files::isDirectory).map(x -> x.toString()).collect(Collectors.toList());

			result.forEach(System.out::println);

		} catch (IOException e) {
			System.out.println("IOException raised at listALLFolders" + e);
		}
	}

	public static List<String> listAllEndsWith(String EndsWithPattern, String FolderPath) {

		List<String> result = null;

		try (Stream<Path> walk = Files.walk(Paths.get(FolderPath))) {

			result = walk.map(x -> x.toString()).filter(f -> f.endsWith(EndsWithPattern)).collect(Collectors.toList());

			result.forEach(System.out::println);

		} catch (IOException e) {
			System.out.println("IO Exception raised at listAllEndsWith method" + e);

		}

		catch (Exception e) {
			System.out.println("Exception raised at listAllEndsWith method" + e);
		}

		return result;
	}

	public static boolean CompareMultiDimList(List<?> list1, List<?> list2) {

		boolean flag = true;
		if (list1 == null) {
			System.out.println("list 1 is null");
			return false;
		}
		if (list2 == null) {
			System.out.println("list 2 is null");
			return false;
		}

		if (list1.size() != list2.size()) {
			System.out.println(
					"List Size is not same. List 1 size :" + list1.size() + "!=" + " List 2 Size :" + list2.size());
			return false;
		} else {

			for (int i = 0; i < list2.size(); i++) {
				if (list1.get(i).equals(list2.get(i))) {
					System.out.println(list1.get(i) + " = " + list2.get(i));
				} else {
					System.out.println(list1.get(i) + " != " + list2.get(i));
					flag = false;
					break;

				}
			}
			if (flag == false) {

				return false;

			}
		}
		return true;
	}

	public static boolean isNullOrEmptyString(String str) {
		if (str != null && !str.isEmpty())
			return false;
		return true;
	}

	
	public static void takeScreenShot() {
		File scrnFile = ((TakesScreenshot) driver).getScreenshotAs(OutputType.FILE);
		String screenShotFolder = System.getProperty("user.dir")+"/ScreenShots/";
		
		try {
			FileUtils.copyFile(scrnFile,new File (screenShotFolder+"Screenshot_"+System.currentTimeMillis()+".png"));
		} catch (IOException e) {
			
			System.out.println("Captured IOException at takeScreenShot method "+e);
		}
		
	}
	
	
	
	public static Object[][] getData(String FileName, String sheetName){
		
		FileInputStream file=null;
		Workbook book;
		Sheet sheet;
		Object[] [] data = null ;
		
		//C:\Users\anudeb\TestData.xlsx
		String path =System.getProperty("user.home")+"\\Desktop\\"+FileName;
		System.out.println(path);
		
		try {
		file = new FileInputStream(path);

		book = WorkbookFactory.create(file);
		sheet = book.getSheet(sheetName);
		
		int rowValue = sheet.getLastRowNum();
		int colValue = sheet.getRow(0).getPhysicalNumberOfCells();
		
		data = new Object[rowValue][colValue];
		
		for(int row =0;row<rowValue;row++) {
			for(int col =0;col<colValue;col++) {
				
				//System.out.println();
				
				String Value = sheet.getRow(row+1).getCell(col).toString();
				
				//System.out.println(Library.isNumeric(Value));
				
				
				
				data[row][col] = Value;
				
				//System.out.println(row +"==="+col+"==="+data[row][col]);
				
				
				
			}
			
			
			
			
		}
		
		
		
		
		
		
		}catch(FileNotFoundException e) {
			System.out.println("Check File Path, it should be in Desktop directory");	
			Assert.fail();
		}
		catch(InvalidFormatException e) {
			e.printStackTrace();
			Assert.fail();
		}
		catch(IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
		
		
		return data;	
		
	}
	
}
