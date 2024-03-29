package com.commonutils;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files ;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.imageio.ImageIO;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.PDFTextStripperByArea;
import org.assertj.core.api.SoftAssertions;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.Keys;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.FluentWait;
import org.openqa.selenium.support.ui.Select;
import org.openqa.selenium.support.ui.WebDriverWait;

import com.cucumber.listener.Reporter;

import cucumber.api.DataTable;
import cucumber.api.Scenario;
import ru.yandex.qatools.ashot.AShot;
import ru.yandex.qatools.ashot.Screenshot;
import ru.yandex.qatools.ashot.shooting.ShootingStrategies;

public class CommonUtils extends BasePage {
	private static final org.apache.logging.log4j.Logger Log = LogManager
			.getLogger(MethodHandles.lookup().lookupClass());
	public static SoftAssertions softAssert = new SoftAssertions();
	public static Properties prop = new Properties();
	static LombardUtils lomUtils = new LombardUtils();

	public static String getCurrentDate() {

		DateFormat dateFormat = new SimpleDateFormat("dd_MM_yyyy_HHmmss");

		Date date = new Date();

		String currentDate = dateFormat.format(date);

		return currentDate;
	}

	/**
	 * This method has been written to capture Screenshot
	 */
	public static void captureScreenshot() {
		String imageFileName = GlobalVariable.scenario.getName().replaceAll(" ", "") + "_" + getCurrentDate() + ".png";
		imageFileName = imageFileName.replaceAll(":", "");
		
		
		File imageFile = new File(System.getProperty("user.dir") + "\\target\\extent\\images\\" + imageFileName);
		try {
			File screen = ((TakesScreenshot) driver).getScreenshotAs(OutputType.FILE);
			com.google.common.io.Files.createParentDirs(imageFile);
			com.google.common.io.Files.touch(imageFile);

			BufferedImage img = ImageIO.read(screen);
			ImageIO.write(img, "png", imageFile);
			Thread.sleep(2000); // file will take time in writing
			Reporter.addScreenCaptureFromPath("images/" + imageFileName);
		
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}


	}
	
	  //Screenshot
		public void captureScreenshotFull() {
			
			String imageFileName = GlobalVariable.scenario.getName().replaceAll("[^a-zA-Z0-9]", "") + "_" + getCurrentDate() + ".png";
		
			
			
			//imageFileName = imageFileName.replaceAll(":", "");
			try {
			Screenshot s=new AShot().shootingStrategy(ShootingStrategies.viewportPasting(1000)).takeScreenshot(driver);
				
			ImageIO.write(s.getImage(),"PNG",new File(System.getProperty("user.dir") + "\\target\\extent\\images\\" + imageFileName));
			Reporter.addScreenCaptureFromPath("images/" + imageFileName);

			}  catch (IOException e) {
				e.printStackTrace();
				}

		}

	public static boolean getMethod(Class<?> className, String testName) {

		// Collection<Method> metds = new ArrayList<Method>();
		boolean mStatus = false;
		while (className != null) {
			for (Method metds : className.getDeclaredMethods()) {
				if (metds.getName().equals(testName)) {
					mStatus = true;
					break;
				}

			}
		}

		return mStatus;
	}

	/**
	 * This method has been written to print any given text in report
	 *
	 * @param PrintMessage
	 *            - Message to be displayed in Report
	 * @param Status
	 *            - Pass/Fail
	 * @throws IOException
	 * @author durgamv
	 */
	public static void printText(String PrintMessage, String Status, boolean Screenshot) {
		if (Status.equalsIgnoreCase("Pass")) {
			softAssert.assertThat(true);
			Reporter.addStepLog("<p style='background-color:#a1c683;'><font color='green'><b>Assertion Pass</b></font>"
					+ "\n" + "<font color='green'><b>" + PrintMessage + "</b></font></p>");
			if (Screenshot == true) {
				captureScreenshot();
			}

		} else if (Status.equalsIgnoreCase("Fail")) {
			softAssert.fail("Assertion failure");
			Reporter.addStepLog("<p style='background-color:#ff9a9a;'><font color='red'><b>Assertion Failure</b></font>"
					+ "\n" + "<font color='red'><b>" + PrintMessage + "</b></font></p>");
			captureScreenshot();

		}
	}

	/**
	 * This method has been written to click the link with Link Text name
	 */

	public void clickByLinkText(String linkText) {
		WaitUtils.waitForElement("//a[text()='" + linkText + "']", Constants.xpath);
		lomUtils.clickOnElementwithWait("//a[text()='" + linkText + "']", Constants.xpath);
		Log.info("Link has been clicked---" + linkText);
		WaitUtils.sleepTime(1);
	}

	/**
	 * This method has been written to click the button with button text name
	 */

	public void clickByButtonText(String buttonText) {

		WaitUtils.waitForElementPresentAndClickAble(driver, By.xpath("//button[text()='" + buttonText + "']"), 10);
		lomUtils.clickOnElementwithWait("//button[text()='" + buttonText + "']", Constants.xpath);
		Log.info("Button has been clicked--- " + buttonText);

	}

	public void scrollUp(String element) {

		WebElement ele = driver.findElement(By.xpath(element));

		JavascriptExecutor jse = (JavascriptExecutor) driver;

		jse.executeScript("arguments[0].scrollIntoView(true);", ele);

	}

	public void scrollToElement(String element, String locType) {

		WebElement ele;

		if (locType.equalsIgnoreCase("id")) {
			ele = driver.findElement(By.id(element));

			JavascriptExecutor jse = (JavascriptExecutor) driver;

			jse.executeScript("arguments[0].scrollIntoView(true);", ele);

		}

		if (locType.equalsIgnoreCase("xpath")) {
			ele = driver.findElement(By.xpath(element));

			JavascriptExecutor jse = (JavascriptExecutor) driver;

			jse.executeScript("arguments[0].scrollIntoView(true);", ele);

		}
	}

	
	
	public void moveTopOfList(String value) {

		driver.findElement(By.xpath("//ul//li[text()='" + value + "']")).sendKeys(Keys.HOME);

		/*
		 * if(locType.equalsIgnoreCase("id")){
		 *
		 * driver.findElement(By.id(element)).sendKeys(Keys.HOME); }else
		 * if(locType.equalsIgnoreCase("xpath")){
		 * driver.findElement(By.xpath(element)).sendKeys(Keys.HOME); }
		 */

	}

	public void setValueOnDropList(String element) {

		WebElement ele = driver.findElement(By.xpath(element));

		JavascriptExecutor jse = (JavascriptExecutor) driver;

		jse.executeScript("arguments[0].innerText=''", ele);

	}

	public void selectDropText(String element, String locType, int indexPosition) {

		Select select;

		if (locType.equalsIgnoreCase("id")) {

			select = new Select(driver.findElement(By.id(element)));
			select.selectByIndex(indexPosition);
		} else if (locType.equalsIgnoreCase("xpath")) {

			select = new Select(driver.findElement(By.xpath(element)));
			select.selectByIndex(indexPosition);
		}

	}

	public void deSelectDropValue() {

		Select select = new Select(driver.findElement(By.id("EntityType")));
		select.selectByIndex(0);
		;
	}

	public void clickByAnyLocator(String elementName, By by) {
		// button[text()='Next']
		waitForLoading();

		try {
			WebElement eleToClick = findAnyWebElement(by, elementName);
			if (eleToClick != null) {
				validateAndClick(elementName, eleToClick);
				// click(elementName, by);
				Log.info(elementName + " has been clicked---");
			}
		} catch (Exception e) {

			Log.info(elementName + " could not be clicked---");
			e.printStackTrace();
		}
		try {
			WebElement eleToClick = findAnyWebElement(by, elementName);
			if (eleToClick != null) {
				validateAndClick(elementName, eleToClick);
				// click(elementName, by);
				Log.info(elementName + " has been clicked---");
			}
		} catch (Exception e) {

			Log.info(elementName + " could not be clicked---");
			e.printStackTrace();
		}

	}

	public String getAnyDate_dateFormat(String dateFormat, int Days) {
		DateFormat df = new SimpleDateFormat(dateFormat);
		df.setTimeZone(TimeZone.getTimeZone("GMT"));
		String oldDate = df.format(new Date());
		Calendar c = Calendar.getInstance();
		try {
			c.setTime(df.parse(oldDate));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		// Number of Days to add
		c.add(Calendar.DAY_OF_MONTH, Days);
		String newDate = df.format(c.getTime());
		System.out.println("Date after Addition: " + newDate);
		return newDate;
	}

	public void clickWorkflowButton(String headerTxt, String buttonTxt) {

		WaitUtils.waitForElement(
				"//li/label[contains(.,'" + headerTxt + "')]/following::button/span[text()='" + buttonTxt + "']",
				Constants.xpath);
		lomUtils.waitForElementToClickable(buttonTxt, By.xpath(
				"//li/label[contains(.,'" + headerTxt + "')]/following::button/span[text()='" + buttonTxt + "']"));
		findAnyWebElement(By.xpath(
				"//li/label[contains(.,'" + headerTxt + "')]/following::button/span[text()='" + buttonTxt + "']"),
				buttonTxt).click();

		Log.info("Button has been clicked--- " + buttonTxt);
	}

	/**
	 * This method has been written to get current date with time stamp.
	 *
	 * @return
	 */

	public String getCurrentDateWithExpectFormat(String expDateFormat) {

		DateFormat dateFormat = new SimpleDateFormat(expDateFormat);

		Date date = new Date();

		String currentDate = dateFormat.format(date);

		return currentDate;
	}

	/**
	 * This method has been written to validate expected and actual text using
	 * assertion.
	 */

	public void assertText(String expected, String actual) {
		Log.info("Assert Text ? --> Expected --" + expected + "    actual --" + actual);
		if (actual.contains(expected)) {
			Log.info("Assert Pass");
			softAssert.assertThat(true);
			Reporter.addStepLog(
					"<p style='background-color:#a1c683;'><font color='green'><b>Assertion Passed</b></font><br>" + "\n"
							+ "<b>Expected: " + "\n<font color='green'>" + expected + "<br></b></font>" + "\n"
							+ "<b>Actual: " + "\n<font  color='green'>" + actual + "<br></b></font></p>");

		} else {
			Log.error("Assert Fail");
			softAssert.fail("Assertion failure");
			Reporter.addStepLog(
					"<p style='background-color:#ff9a9a;'><font color='red'><b>Assertion Failure</b></font><b>Expected: "
							+ "<font color='green'>" + expected + "</b></font>" + "\n" + "<b>Actual: <font color='red'>"
							+ actual + "</b></font></p>");

		}
		//captureScreenshot();
		captureScreenshotFull();
	}

	/**
	 * This method has been written to validate expected and actual text using
	 * assertion.
	 */

	public void assertValidation(boolean assertStatus) {
		if (assertStatus == true) {
			softAssert.assertThat(true);
		} else {
			softAssert.fail("Assertion failure");
		}
		captureScreenshot();
	}

	/**
	 * This method has been written to click TAB key from any input field.
	 *
	 * @param element
	 * @param locType
	 */

	public void clickTabKey(String element, String locType) {

		if (locType.equalsIgnoreCase("id")) {
			driver.findElement(By.id(element)).sendKeys(Keys.TAB);
		} else if (locType.equalsIgnoreCase("xpath")) {
			driver.findElement(By.xpath(element)).sendKeys(Keys.TAB);
		}
	
	}

	public void selectCurrentDateInAnyCalendarPopup(String text, String xpath) {
		try {

			wait(1000);
			// Select the required date
			String enteredDate1 = getAnyDate_dateFormat("MMMM dd", 0);
			String enteredDate2 = getAnyDate_dateFormat("dd MMMM", 0);
			lomUtils.clickOnElement(xpath, Constants.xpath);
			clickByAnyLocator("Calendar", By.xpath("//div[@id='" + text + "']//table//td/a[contains(@title,'"
					+ enteredDate1 + "') or contains(@title,'" + enteredDate2 + "')]"));
			System.out.println("//div[@id='" + text + "']//table//td/a[contains(@title,'" + enteredDate1
					+ "') or contains(@title,'" + enteredDate2 + "')]");
		} catch (Exception e) {

			e.printStackTrace();
		}

	}

	/**
	 * This method has been written to get text from element
	 *
	 * @param element
	 * @param locType
	 * @return
	 */
	public String getTextFromElement(String element, String locType) {

		String getText = null;

		if (locType.equalsIgnoreCase("id")) {
			getText = driver.findElement(By.id(element)).getText();
		} else if (locType.equalsIgnoreCase("xpath")) {
			getText = driver.findElement(By.xpath(element)).getText();
		}

		return getText;
	}

	/**
	 * This method has been written to get text from element
	 *
	 * @param element
	 * @param locType
	 * @return
	 */
	public String getValueFromElement(String element, String locType) {

		String getText = null;
		WaitUtils.waitForElement(element, locType);
		if (locType.equalsIgnoreCase("id")) {
			getText = driver.findElement(By.id(element)).getAttribute("value");
		} else if (locType.equalsIgnoreCase("xpath")) {
			getText = driver.findElement(By.xpath(element)).getAttribute("value");
		}

		return getText;
	}

	public void verifyWorkflowState(String headerTxt, String buttonTxt) {

		try {
			if (driver.findElements(By.xpath(
					"//li/label[contains(.,'" + headerTxt + "')]/following::button/span[text()='" + buttonTxt + "']"))
					.size() > 0) {
				Log.info("Workflow State present--- " + buttonTxt);
			}
		} catch (Exception e) {
			Log.info("Workflow State absent--- " + buttonTxt);
			e.printStackTrace();
		}
	}



	/**
	 * This method has been written to verify the error message not present on
	 * the page
	 *
	 * @throws IOException
	 */

	public void verifyElementNotPresentOnPage(String element, String locType) {

		String destPath = GlobalVariable.scenario.getName().replaceAll(" ", "") + "_" + getCurrentDate() + ".png";

		if (locType.equalsIgnoreCase("id")) {

			try {
				if (driver.findElements(By.id(element)).size() == 0) {
					softAssert.assertThat(true);
					captureScreenshot();
					Reporter.addScreenCaptureFromPath(destPath);
				}
				softAssert.assertThat(true);
				captureScreenshot();
				Reporter.addScreenCaptureFromPath(destPath);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * This method has been written to close current window and Switch to
	 * selected window
	 *
	 * @param PageTitle
	 * @
	 */
	public void closeAndSwitchToWindow(String PageTitle) {

		WaitUtils.sleepTime(3);
		Log.info("Closing " + driver.getCurrentUrl());
		GlobalVariable.tab = GlobalVariable.tab - 1;
		driver.close();
		WaitUtils.sleepTime(2);
		switchToWindow(PageTitle);
	}

	/**
	 * This method has been written to Switch to selected window
	 */
	public void switchToWindow(String Title) {

		WaitUtils.sleepTime(3);
		Set<String> handles = driver.getWindowHandles();
		Iterator<String> I1 = handles.iterator();
		int size = handles.size();
		for (int i = 1; i <= size; i++) {
			WaitUtils.sleepTime(1);
			driver.switchTo().window(I1.next());
			WaitUtils.sleepTime(1);
			String title = driver.getTitle();
			System.out.println("window displayed is " + title);
			if (title.contains(Title)) {
				i = size + 1;
				System.out.println("Current window is " + driver.getTitle());
			}
		}
		Log.info("Current Window is " + driver.getCurrentUrl());
	}

	public void clickWorkflowButton(String headerTxt, String buttonTxt1, String buttonTxt2) {

		lomUtils.waitForLoading();
		lomUtils.clickOnElementwithWait("//li/label[contains(.,'" + headerTxt + "')]/following::button/span[text()='"
				+ buttonTxt1 + "' or text()='" + buttonTxt2 + "']", Constants.xpath);
		// lomUtils.waitForElementToClickable(buttonTxt1,
		// By.xpath("//li/label[contains(.,'" + headerTxt
		// + "')]/following::button/span[text()='" + buttonTxt1 + "' or
		// text()='" + buttonTxt2 + "']"));
		// findAnyWebElement(By.xpath("//li/label[contains(.,'" + headerTxt +
		// "')]/following::button/span[text()='"
		// + buttonTxt1 + "' or text()='" + buttonTxt2 + "']"),
		// buttonTxt1).click();

		Log.info("Button has been clicked--- " + buttonTxt1);
	}

	/**
	 * This method has been written to validate the expected values in drop
	 * down.
	 */

	public void verifyDropDownValuesAvailaiblity(DataTable table, String element) {

		String expected, actual;

		Select select = new Select(driver.findElement(By.xpath(element)));

		for (Map<String, String> object : table.asMaps(String.class, String.class)) {

			List<WebElement> options = select.getOptions();

			for (WebElement option : options) {

				expected = object.get("dropValue").toString();
				actual = option.getText();
				assertText(expected, actual);
			}
		}

	}

	/**
	 * This method has been written to validate the error message
	 */

	public void validationMessageValidation(String expMsg) {
		Log.info("Verify message: " + expMsg);
		// WaitUtils.waitForJQueryProcessing(driver,Constants.MED_WAIT_SECS);

		String actual, newExpError = null;
		if (expMsg.contains(" ")) {
			newExpError = expMsg.replace(" ", "£");
			try {
				if (driver.findElement(By.id("toast-container")).isDisplayed()) {
					actual = driver.findElement(By.id("toast-container")).getText();
					try {
						assertText(newExpError, actual);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} catch (NoSuchElementException e) {
				Reporter.addStepLog("Message not found on the page");
			}
		} else {
			try {
				if (driver.findElement(By.id("toast-container")).isDisplayed()) {
					actual = driver.findElement(By.id("toast-container")).getText();
					try {
						assertText(expMsg, actual);
						Log.info("Validate message " + expMsg + " success");
					} catch (Exception e) {

						e.printStackTrace();
					}
				}
			} catch (NoSuchElementException e) {

				Reporter.addStepLog("Message not found on the page");
			}
		}
		captureScreenshot();
	}

	/***
	 * This method is to get text in Drop down
	 * 
	 * @param expLabelName
	 * @return
	 * @author durgamv
	 */
	public String getTextInDropDown(String expLabelName) {
		waitForLoading();
		lomUtils.waitForLoadingElementInvisibility(driver);
		String value = null;
		try {
			WaitUtils.waitForElement(
					"//label[(text()='" + expLabelName + "')]/following-sibling::div//span[@class='k-input']",
					Constants.xpath);
			By by = By.xpath("//label[(text()='" + expLabelName + "')]/following-sibling::div//span[@class='k-input']");
			lomUtils.waitForElementToVisible(expLabelName, by);
			lomUtils.moveToElement(by);
			value = driver.findElement(by).getText();
			Log.info("Get text from Drop down = " + value);

		} catch (Exception e) {
			Log.info("exception occured while getting value in---" + expLabelName);
			e.printStackTrace();
		}
		return value;
	}

	/**
	 * This method has been written to validate the Expected and Actual text
	 * then update result in report
	 *
	 * @param ExpectedText
	 *            - Expected Field/Scenario
	 * @param ExpectedValue
	 *            - Expected value to be validate
	 * @param ActualText
	 *            - Actual Field/Scenario
	 * @param ActualValue
	 *            - Actual value to be validate
	 * @throws IOException
	 * @author durgamv
	 */
	public void validateText(String ExpectedText, String ExpectedValue, String ActualText, String ActualValue) {
		if (ExpectedValue.equalsIgnoreCase(ActualValue)) {
			softAssert.assertThat(true);
			Reporter.addStepLog("<p style='background-color:#a1c683;'><font color='green'><b>Assertion Pass</b></font>"
					+ "\n" + "<b>Expected " + ExpectedText + ": " + "<font color='green'>" + ExpectedValue
					+ "</b></font>" + "\n" + "<b>Actual " + ActualText + ": " + "<font color='green'>" + ActualValue
					+ "</b></font></p>");
			captureScreenshot();

		} else {
			softAssert.fail("Assertion failure");
			Reporter.addStepLog("<p style='background-color:#ff9a9a;'><font color='red'><b>Assertion Failure</b></font>"
					+ "\n" + "<b>Expected " + ExpectedText + ": " + "<font color='green'>" + ExpectedValue
					+ "</b></font>" + "\n" + "<b>Actual " + ActualText + ": " + "<font color='red'>" + ActualValue
					+ "</b></font></p>");
			captureScreenshot();

		}
	}

	public void doubleClickOnElement(String element, String locType) {

		Actions action = new Actions(driver);
		WebElement webElement = driver.findElement(By.xpath(element));
		action.moveToElement(webElement).doubleClick().perform();
	}

	/**
	 * This method has been written to assert a text in text file
	 *
	 * @param expKey
	 * @param FilePath
	 * @throws IOException
	 * @author Ranjith
	 */
	public void assertTextFromFile(String expKey, String FilePath) {

		String file = System.getProperty("user.dir") + FilePath;
		String line = null;
		FileReader filereader = null;
		BufferedReader bufferfile = null;
		try {

			filereader = new FileReader(file);
			bufferfile = new BufferedReader(filereader);

			while ((line = bufferfile.readLine()) != null) {

				if (!line.isEmpty()) {
					if (line.contains(expKey)) {
						softAssert.assertThat(true);
						break;
					}
				}
			}

		} catch (AssertionError e) {
			softAssert.assertThat(false);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bufferfile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	/***
	 * This method has been written to run the Batch file in the given path
	 * 
	 * @param BatFilePath
	 *            - In current directory
	 * @throws IOException
	 * @author durgamv
	 */
	public void runBatchFile(String BatFilePath) {

		String path = "cmd /c start " + System.getProperty("user.dir") + BatFilePath;
		Runtime rn = Runtime.getRuntime();
		try {
			Process pr = rn.exec(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/***
	 * This method has been written to clear data in file
	 * 
	 * @param filePath
	 *            - With/without extension
	 * @throws IOException
	 * @author durgamv
	 */
	public void clearFileData(String filePath) {

		FileWriter writer = null;
		try {
			writer = new FileWriter(System.getProperty("user.dir") + filePath, false);

			writer.write("");
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String getCurrentDateUntilHours() {

		DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH");

		Date date = new Date();

		String currentDate = dateFormat.format(date);

		return currentDate;
	}

	public void batchFileRunner(String BatFilePath) {
		// String path="cmd /c start d:\\Users\\raoyp\\LatestFileCopy.bat";
		String path = "cmd /c start " + BatFilePath;
		Runtime rn = Runtime.getRuntime();
		try {
			Process pr = rn.exec(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String LastmodifiedDateTime() {

		File file = new File("c:\\Temp\\New Text Document.txt");

		System.out.println("Before Format : " + file.lastModified());

		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH");

		System.out.println("After Format : " + sdf.format(file.lastModified()));
		/*
		 * long modifiedTimeDate= file.lastModified(); return modifiedTimeDate;
		 */
		return sdf.format(file.lastModified());
	}

	public void subtractTime() {

		if (getCurrentDateUntilHours().contains(LastmodifiedDateTime())) {
			System.out.println("test case pass");

		}

	}

	public void validationMessageValidationV2(String expMsg) {
		Log.info("Verify message: " + expMsg);
		// WaitUtils.waitForJQueryProcessing(driver,Constants.MED_WAIT_SECS);

		String actual, newExpError = null;
		if (expMsg.contains(" ")) {
			newExpError = expMsg.replace(" ", "£");
			try {
				if (driver.findElement(By.id("toast-container")).isDisplayed()) {
					actual = driver.findElement(By.id("toast-container")).getText();
					assertText(newExpError, actual);
				}
			} catch (NoSuchElementException e) {
				Reporter.addStepLog("Message not found on the page");
			}
		} else {
			try {
				if (driver.findElement(By.id("toast-container")).isDisplayed()) {
					actual = driver.findElement(By.id("toast-container")).getText();
					assertText(expMsg, actual);
					Log.info("Validate message " + expMsg + " success");
				}
			} catch (NoSuchElementException e) {

				Reporter.addStepLog("Message not found on the page");
			}
		}
		captureScreenshot();
	}

	public void uploaderReport(int expected, int actual) {
		Log.info("Assert Text ? -- Expected " + expected + "    actual --" + actual);
		if (actual == expected) {
			softAssert.assertThat(true);
			Reporter.addStepLog(
					"<p><font color='green'><b>Passed: All assets and contracts contains expected values </b></font>"
							+ "\n" + "<b>Expected: " + "<font color='green'>" + expected + "</b></font>" + "\n"
							+ "<b>Actual: " + "<font  color='green'>" + actual + "</b></font></p>");

		} else {

			softAssert.fail("Assertion failure");
			Reporter.addStepLog(
					"<p style='background-color:#ff9a9a;'><font color='red'><b>Assertion Failure</b></font><b>Expected Assets/Contracts To Be Passed: "
							+ "<font color='green'>" + expected + "</b></font>" + "\n"
							+ "<b>Actual Assets/Contracts Passed: <font color='red'>" + actual + "</b></font></p>");

		}
		// captureScreenshot();
	}

	public String getAtributeValue(String ele) {

		WebElement inputBox = driver.findElement(By.xpath(ele));
		String textInsideInputBox = inputBox.getAttribute("value");
		return textInsideInputBox;

	}

	public void waitForLoadingDigitalLombard() {
		/*
		 * try { new WebDriverWait(driver,
		 * 20).until(ExpectedConditions.invisibilityOfElementLocated(By.
		 * cssSelector(".zb-loader"))); Log.info("Waited for loading----Done");
		 * } catch (Exception e) { Log.info(
		 * "Waited for loading----NOt Successfull---Reinstated Implicit wait");
		 * e.printStackTrace(); }
		 */
		
	}

	public static String getDateOfToday() {

		DateFormat dateFormat = new SimpleDateFormat("dd_MM_yyyy");

		Date date = new Date();

		return dateFormat.format(date);
	}

	public void assertTextIsNotNull(String expected, String actual) {
		Log.info("Assert Text ? -- Expected " + expected + "    actual --" + actual);
		if (!actual.isEmpty()) {
			softAssert.assertThat(true);
			Reporter.addStepLog(
					"<p style='background-color:#a1c683;'><font color='green'><b>Assertion Passed</b></font><br>" + "\n"
							+ "<b>Expected: " + "\n<font color='green'>" + expected + "<br></b></font>" + "\n"
							+ "<b>Actual: " + "\n<font  color='green'>" + actual + "<br></b></font></p>");

		} else {

			softAssert.fail("Assertion failure");
			Reporter.addStepLog(
					"<p style='background-color:#ff9a9a;'><font color='red'><b>Assertion Failure</b></font><b>Expected: "
							+ "<font color='green'>" + expected + "</b></font>" + "\n" + "<b>Actual: <font color='red'>"
							+ actual + "</b></font></p>");

		}
		captureScreenshot();
	}

	public boolean testDataValueNotNullCheak(String value) {
		if (value != null)
			if (!value.equalsIgnoreCase("null") && value.length() != 0)
				return true;
			else
				return false;
		else
			return false;
	}

	/**
	 * This method has been written to click on element by using element and
	 * locType
	 */
	public boolean isElementVisible(String element, String locType, int waitTime) {
		// nullify implicit wait
		driver.manage().timeouts().implicitlyWait(0, TimeUnit.SECONDS);
		boolean status = false;
		try {
			By by = returnByfromLocator(element, locType);
			WebDriverWait wait = new WebDriverWait(driver, waitTime);
			wait.until(ExpectedConditions.visibilityOfElementLocated(by));
			status = true;
			WaitUtils.sleepTime(1);
			driver.manage().timeouts().implicitlyWait(
					Integer.parseInt(GlobalVariable.executionProps.getProperty("DEFAULT_WAIT_4_PAGE_SEC")),
					TimeUnit.SECONDS);
		} catch (Exception e) {
			status = false;
			Log.info(element + " is not present : " + e);
		}
		return status;
	}

	public By returnByfromLocator(String element, String locType) {
		By by = null;
		if (locType.equalsIgnoreCase("id")) {
			by = By.id(element);
		}
		if (locType.equalsIgnoreCase("xpath")) {
			by = By.xpath(element);
		}
		if (locType.equalsIgnoreCase("linktext")) {
			by = By.linkText(element);
		}
		return by;
	}

	public void switchToNewWindow(String windowName) {
		for (String handler : driver.getWindowHandles()) {
			if (!(handler.equals(windowName))) {
				driver.switchTo().window(handler);
			}
		}
	}

	public void switchToWindowWithTitle(String your_title) {
		String currentWindow = driver.getWindowHandle(); // will keep current
															// window to switch
															// back
		for (String winHandle : driver.getWindowHandles()) {
			Log.info("Window : " + driver.switchTo().window(winHandle).getTitle());
			if (driver.switchTo().window(winHandle).getTitle().contains(your_title)) {
				// driver.switchTo().window(winHandle);
				System.out.println("Went to Window Having Title as " + your_title);
				break;
			} else {
				driver.switchTo().window(currentWindow);
			}
		}
	}

	public void addLogToReport(String logText) {

		Log.info(logText);
		Reporter.addStepLog(
				"<p style='background-color:#a1c683;'><font color='green'><b>" + logText + "</b></font><br>");

	}

	public static void validateTextWithoutAssertion(String ExpectedText, String ExpectedValue, String ActualText,
			String ActualValue, boolean AssertionFailure) {
		if (ExpectedValue.equalsIgnoreCase(ActualValue)) {
			softAssert.assertThat(true);
			Reporter.addStepLog("<p style='background-color:#a1c683;'><font color='green'><b>Assertion Pass</b></font>"
					+ "\n" + "<b>" + ExpectedText + ": " + "<font color='green'>" + ExpectedValue + "</b></font>" + "\n"
					+ "<b>" + ActualText + ": " + "<font color='green'>" + ActualValue + "</b></font></p>");

		} else {
			if (AssertionFailure == true) {
				softAssert.fail("Assertion failure");
			} else {
				softAssert.assertThat(true);
			}
			Reporter.addStepLog("<p style='background-color:#ff9a9a;'><font color='red'><b>Fail</b></font>" + "\n"
					+ "<b>" + ExpectedText + ": " + "<font color='green'>" + ExpectedValue + "</b></font>" + "\n"
					+ "<b>" + ActualText + ": " + "<font color='red'>" + ActualValue + "</b></font></p>");

		}
	}

	public boolean isElementEnabled(String elementName, String locType) {
		boolean status = false;
		By by = returnByfromLocator(elementName, locType);
		waitForLoading();
		try {
			status = driver.findElement(by).isEnabled();
			System.out.println(elementName + " is Enabled " + status);
			Log.info(elementName + " is Enabled " + status);
		} catch (NoSuchElementException e) {
			status = false;
			System.out.println(elementName + " is not Enabled");
			Log.info(elementName + " is not Enabled");
			// return status;
		} catch (Exception e) {
			status = false;
			System.out.println(elementName + " is not Enabled");
			Log.info(elementName + " is not Enabled");
			// return status;
		}
		return status;
	}

	/*
	 * Author: debad
	 * 
	 */

	public static void LoadConfig(String FileName) {
		// String path = System.getProperty("user.d");
		// String fpath = path + "\\Configuration\\" + FileName;
		try {
			FileInputStream inStream = new FileInputStream(FileName);
			prop.load(inStream);
			inStream.close();
		} catch (NullPointerException e) {
			Log.error("Can't load null, Check Path. Cause : ", e);
		} catch (FileNotFoundException e) {
			Log.error("Not Able to Load File, Check Path. Cause : ", e);
		} catch (IOException e) {
			Log.error("IOException, Check Path. Cause : ", e);
		} catch (Exception e) {
			Log.error("Exception raised, Check Path. Cause : ", e);
		}
	}

	public static String getVal(String key) {
		String value = prop.getProperty(key);
		return value;
	}

	/*
	 * This Method will check if the String is null or empty
	 */

	public static boolean checknullString(String text) {

		boolean result = false;
		if (text == null || text.isEmpty() || text.trim().isEmpty()) {
			Log.info("String is null, empty or blank.");
			return true;
		} else
			return result;
	}

	public void clickOnElementwithWait(String element, String locType, int timeInSec) {
		FluentWait<WebDriver> wait = null;
		By by = null;
		Log.info("Wait for Element started :" + element);
		try {
			wait = new FluentWait<WebDriver>(driver)
					.withTimeout(timeInSec, TimeUnit.SECONDS)
					.pollingEvery(1000, TimeUnit.MILLISECONDS)
					.withMessage("Timeout occured!")
					.ignoring(java.util.NoSuchElementException.class);
			if (locType.equalsIgnoreCase("id")) {
				by = By.id(element);
				wait.until(ExpectedConditions.visibilityOfElementLocated(By.id(element)));
				wait.until(ExpectedConditions.elementToBeClickable(By.id(element)));
				
			} else if (locType.equalsIgnoreCase("xpath")) {
				by = By.xpath(element);
				wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(element)));
				wait.until(ExpectedConditions.elementToBeClickable(By.xpath(element)));
				
			}
			captureScreenshotFull();
			
			driver.findElement(by).click();
			Log.info("clicked on :" + element);

		} catch (Exception e) {
			captureScreenshotFull();
			Log.error(e);
			// Assert.fail();
		}
	}

	


	
	
	
	
	
	
	public void waitForElementToBeClickable(String element, String locType, int timeInSec) {
		FluentWait<WebDriver> wait = null;

		By by = returnByfromLocator(element, locType);
		Log.info("Wait for Element started :" + element);
		try {
			wait = new FluentWait<WebDriver>(driver)
					.withTimeout(timeInSec, TimeUnit.SECONDS)
					.pollingEvery(1000, TimeUnit.MILLISECONDS)
					.withMessage("Timeout occured!")
					.ignoring(java.util.NoSuchElementException.class);
			if (locType.equalsIgnoreCase("id")) {
				by = By.id(element);
				wait.until(ExpectedConditions.visibilityOfElementLocated(by));
				wait.until(ExpectedConditions.presenceOfElementLocated(by));
				wait.until(ExpectedConditions.elementToBeClickable(by));
			} else if (locType.equalsIgnoreCase("xpath")) {
				by = By.xpath(element);
				wait.until(ExpectedConditions.visibilityOfElementLocated(by));
				wait.until(ExpectedConditions.presenceOfElementLocated(by));
				wait.until(ExpectedConditions.elementToBeClickable(by));
			}

		} catch (Exception e) {
			lomUtils.failCase(e);
		}
	}

	public static void openNewTab() {
		JavascriptExecutor js = (JavascriptExecutor) driver;
		js.executeScript("window.open('');");

	}

	public void switchToNewTab() {
		openNewTab();
		String subWindowHandler = null;

		Set<String> handles = driver.getWindowHandles();
		Iterator<String> iterator = handles.iterator();
		while (iterator.hasNext()) {
			subWindowHandler = iterator.next();
		}
		driver.switchTo().window(subWindowHandler);
	}

	public void closeAllOpenTab(String mainWindow) {

		for (String handler : driver.getWindowHandles()) {
			if (!(handler.equals(mainWindow))) {
				driver.switchTo().window(handler);
				driver.close();
			}
		}
		driver.switchTo().window(mainWindow);
	}

	public void closeAllTheOpenTabs() {

		// Wait has been added since switching between multiple windows
		lomUtils.switchwindwon(0);

		String mainWindow = driver.getWindowHandle();

		closeAllOpenTab(mainWindow);

		driver.switchTo().window(mainWindow);

	}

	
	public static void brokenLinks() {

		// Storing the links in a list and traversing through the links
		List<WebElement> links = driver.findElements(By.tagName("a"));

		// This line will print the number of links and the count of links.
		Log.info("No of links are " + links.size());

		// checking the links fetched.
		Log.info("************Verifying Links******************");
		for (int i = 0; i < links.size(); i++) {
			WebElement E1 = links.get(i);

			if (WebUtils.isAttribtuePresent(E1, "href")) {

				String url = E1.getAttribute("href");
				Log.info("Url : " + url + " -> Element : " + E1.getText());

				if (url == null || url.isEmpty()) {
					softAssert.fail("Assertion failure: URL is Empty");
					Reporter.addStepLog(
							"<p style='background-color:#ff9a9a;'><font color='red'><b>Assertion Failure</b></font>"
									+ "\n" + "<font color='red'><b>" + url + " is a broken link/Empty"
									+ "</b></font></p>");
				} else {
					if (!url.contains("tel") || !url.contains("mail_to") || !url.contains("javascript")) {
						if (CommonUtils.getVal("Proxy_Flag").equalsIgnoreCase("TRUE"))
							verifyLinkswithProxy(url);
						else
							verifyLinks(url);
					} else
						Log.info("Email address or Telephone detected in the link. URL :" + url);
				}
			}
		}

	}

	public static void verifyLinkswithProxy(String linkUrl) {
		try {
			URL url = new URL(linkUrl);

			// Now we will be creating url connection and getting the response
			// code
			HttpURLConnection httpURLConnect = (HttpURLConnection) url.openConnection();
			httpURLConnect.setConnectTimeout(5000);

			Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(CommonUtils.getVal("Proxy"), 8080));
			httpURLConnect = (HttpURLConnection) url.openConnection(proxy);

			httpURLConnect.connect();
			if (httpURLConnect.getResponseCode() >= 400) {
				Log.error(linkUrl + " - " + httpURLConnect.getResponseMessage() + "is a broken link");

				softAssert.fail("Assertion failure");
				Reporter.addStepLog(
						"<p style='background-color:#ff9a9a;'><font color='red'><b>Assertion Failure</b></font>" + "\n"
								+ "<font color='red'><b>" + url + " is a broken link" + "</b></font></p>");
			}

			// Fetching and Printing the response code obtained
			else {
				Log.info(linkUrl + " - " + httpURLConnect.getResponseMessage());
			}
		} catch (Exception e) {
			Log.error(e);
		}
	}

	
	public static void verifyLinks(String linkUrl) {
		try {
			URL url = new URL(linkUrl);

			// Now we will be creating url connection and getting the response
			// code
			HttpURLConnection httpURLConnect = (HttpURLConnection) url.openConnection();
			httpURLConnect.setConnectTimeout(5000);

			httpURLConnect.connect();
			if (httpURLConnect.getResponseCode() >= 400) {
				Log.error(linkUrl + " - " + httpURLConnect.getResponseMessage() + "is a broken link");

				softAssert.fail("Assertion failure");
				Reporter.addStepLog(
						"<p style='background-color:#ff9a9a;'><font color='red'><b>Assertion Failure</b></font>" + "\n"
								+ "<font color='red'><b>" + url + " is a broken link" + "</b></font></p>");
			}

			// Fetching and Printing the response code obtained
			else {
				Log.info(linkUrl + " - " + httpURLConnect.getResponseMessage());
			}
		} catch (Exception e) {
			Log.error(e);
		}
	}

	
	
	
	public static String getRandomEmail() {
		String CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
		StringBuilder randStr = new StringBuilder();
		Random rnd = new Random();
		while (randStr.length() < 10) { // length of the random string.
			int index = (int) (rnd.nextFloat() * CHARS.length());
			randStr.append(CHARS.charAt(index));
		}
		

		return randStr.toString() + "@gmail.com";

	}

	public static String getRandomMobile() {
		String CHARS = "1234567890";
		StringBuilder randStr = new StringBuilder();
		Random rnd = new Random();
		while (randStr.length() < 10) { // length of the random string.
			int index = (int) (rnd.nextFloat() * CHARS.length());
			randStr.append(CHARS.charAt(index));
		}
		String randMobile = randStr.toString();

		return randMobile;

	}
	
	public static String getRandomString() {
		String chars = "abcdefghijklmnopqrstuvwxyz";
		StringBuilder randStr = new StringBuilder();
		Random rnd = new Random();
		while (randStr.length() < 10) { // length of the random string.
			int index = (int) (rnd.nextFloat() * chars.length());
			randStr.append(chars.charAt(index));
		}
		return "Aut"+randStr.toString();
	}
	    
	    
	    
	    

	public static void sendKeys(WebElement element, int timeout, String value) {
		new WebDriverWait(driver, timeout).until(ExpectedConditions.visibilityOf(element));
		element.sendKeys(value);
	}

	public void enterTextInInputwithDelete(String element, String locType, String value) {
		try {
			Log.info("Initial Value *************enterTextInInputwithDelete********************** " + value);
			WaitUtils.waitForElement(element, locType);
			WebElement ele = null;
			driver.manage().timeouts().implicitlyWait(5, TimeUnit.SECONDS);
			if (locType.equalsIgnoreCase("id") && !(value.equalsIgnoreCase("null"))) {
				ele = driver.findElement(By.id(element));
			}
			if (locType.equalsIgnoreCase("xpath") && !(value.equalsIgnoreCase("null"))) {
				ele = driver.findElement(By.xpath(element));
			}
			if (locType.equalsIgnoreCase("name") && !(value.equalsIgnoreCase("null"))) {
				ele = driver.findElement(By.name(element));
			}
			sendKeys(ele, 10, Keys.CONTROL + "a" + Keys.DELETE);
			sendKeys(ele, 10, value);
			sendKeys(ele, 10, Keys.TAB + "");

			Log.info("Final Value *************enterTextInInputwithDelete********************** "
					+ ele.getAttribute("value"));
			// if (ele.getAttribute("value").equals(null)) {
			// captureScreenshot();
			// Assert.fail();
			// }
		} catch (Exception e) {
			captureScreenshot();
			e.printStackTrace();
		}
	}
	
	public boolean isElementPresent(String elementName, String locType) {
		boolean status = false;
		By by = returnByfromLocator(elementName, locType);
		try {
			status = driver.findElement(by).isDisplayed();
			status = true;
			
			Log.info(elementName + " is present");
		} catch (NoSuchElementException e) {
			status = false;
			Log.error(elementName + " is not present "+ e);
			;
		} catch (Exception e) {
			status = false;
			Log.error(elementName + " is not present " + e);

			// return status;
		}
		return status;
	}
	

	public boolean isElementPresent(String elementName, By by) {
		boolean status = false;
		waitForLoading();
		try {
			status = driver.findElement(by).isDisplayed();
			status = true;
			Log.info(elementName + " is present");
			Log.info(elementName + " is present");
		} catch (NoSuchElementException e) {
			status = false;
			Log.info(elementName + " is not present");
			Log.info(elementName + " is not present");
			// return status;
		} catch (Exception e) {
			status = false;
			Log.info(elementName + " is not present");
			Log.info(elementName + " is not present");
			// return status;
		}
		return status;
	}

	

	public void closeTab() {
		driver.close();

		if (GlobalVariable.tab > 0)
			GlobalVariable.tab = GlobalVariable.tab - 1;
	}

	public void closeBrowser() {
		driver.quit();

	}
	

	public void scrollUp1(By locator) {

		WebElement ele = driver.findElement(locator);

		JavascriptExecutor jse = (JavascriptExecutor) driver;

		jse.executeScript("arguments[0].scrollIntoView(true);", ele);

	}


	public static String[] readPdf(String path){
		
		PDDocument document;
		String lines[] = null;
		 
	 try {
		 document = PDDocument.load(new File(path));

         document.getClass();

         if (!document.isEncrypted()) {
			
             PDFTextStripperByArea stripper = new PDFTextStripperByArea();
             stripper.setSortByPosition(true);

             PDFTextStripper tStripper = new PDFTextStripper();

             String pdfFileInText = tStripper.getText(document);
             //System.out.println("Text:" + st);

				// split by whitespace
              lines = pdfFileInText.split("\\r?\\n");
             for (String line : lines) {
                 Log.info(line);
                 
                
                 Reporter.addStepLog("<p style='background-color:#a1c683;'><font color='green'><b>" + line + "</b></font></p>"+ "\n" );
                 
                 
                 
             }

         }
	 }catch(Exception e){
		 Log.error(" Exception "+e);
		 e.printStackTrace();
	 }
	 
	return lines;
	}
	
	public static boolean isNullOrEmptyMap(Map<?, ?> map) {
		try {
			if (map == null || map.isEmpty())
				return true;
			else
				return false;
		} catch (NullPointerException e) {
			Log.info("Caught NullPointer Exception at isNullOrEmptyMap method. Check the map");
		}
		return false;
		
	}

	/**
	 * @author Anurag_Deb This method will check the difference of Key's Value in
	 *         two maps
	 * @param map1
	 * @param map2
	 * @return result in boolean format
	 * @throws Exception
	 */

	public static boolean checkDiffMap(Map<String, ?> map1, Map<String, ?> map2) {

		try {

			
			if ((!isNullOrEmptyMap(map1)) && (!isNullOrEmptyMap(map2))) {

				boolean sizeCheck = map1.size() == map2.size();
				Log.info("Verify Size of Maps. Status : " + sizeCheck + "   Map1 Size : " + map1.size()
						+ "   Map2 Size : " + map2.size());
				
				 

				for (Map.Entry<String, ?> me : map2.entrySet()) {
					String key = me.getKey();
					if (map1.containsKey(key)) {

						if (map2.get(key).equals(map1.get(key))) {
							Log.info("Map1 Value :" + map1.get(key) + " =  Map2 Value :" + map2.get(key) + " for Key :"
									+ key);
							Reporter.addStepLog("<p style='background-color:#a1c683;'><font color='green'><b>Assertion Pass</b></font>"
									+ "\n" + "<font color='green'><b>" + "Map1 Value :" + map1.get(key) + " =  Map2 Value :" + map2.get(key) + " for Key :"
									+ key + "</b></font></p>");
							
						} else {
							Log.error("Map1 Value :" + map1.get(key) + " !=  Map2 Value :" + map2.get(key)
									+ " for Key :" + key);
							Reporter.addStepLog("<p style='background-color:#ff9a9a;'><font color='red'><b>Assertion Failure</b></font>"
									+ "\n" + "<font color='red'><b>" + "Map1 Value :" + map1.get(key) + " !=  Map2 Value :" + map2.get(key)
									+ " for Key :" + key + "</b></font></p>");

							return false;
						}
					}
				}
			} else {
				Log.error("One of the Map is Empty. Map1.size :" + map1.size() + " Map2.size :" + map2.size());
				
				Reporter.addStepLog("<p style='background-color:#ff9a9a;'><font color='red'><b>Assertion Failure</b></font>"
						+ "\n" + "<font color='red'><b>" + "One of the Map is Empty. Map1.size :" + map1.size() + " Map2.size :" + map2.size() + "</b></font></p>");
				return false;
			}

		} catch (NullPointerException e) {
			Log.info("Nullpointer Exception caught at checkDiffMap method. Check if map is null.", e);

		} catch (Exception e) {

			Log.info("Exception caught at checkDiffMap method check.", e);
		}

		return true;

	} // End of checkDiffMap method
	
	//Method to get period of diff btwn 2 dates in yeans and month
	public  String[] finddatediff(String addressEffectiveFrom)   
    {   
		String[] eDt = addressEffectiveFrom.split("/");
		String[] cDt= LocalDate.now().toString().split("-");
		 Log.info("Effectivedate"+eDt[2]+" "+eDt[1]+" "+eDt[0]);
		 Log.info("Effectivedate"+cDt[0]+" "+cDt[1]+" "+cDt[2]);
		 
		 LocalDate first_date = LocalDate.of(Integer.valueOf(eDt[2]), Integer.valueOf(eDt[0]), Integer.valueOf(eDt[1]));
		 LocalDate second_date = LocalDate.of(Integer.parseInt(cDt[0]), Integer.parseInt(cDt[1]), Integer.parseInt(cDt[2]));
		
        // Get period between the first and the second date   
        Period difference = Period.between(first_date, second_date);  
        // Show date difference in years, months   
        String test = String.valueOf(difference);
        String[] splitTime = test.split("\\D");
        Log.info("SplitDate&Time--->"+splitTime[1]+" "+splitTime[2]);
        
        return splitTime;  
    }
	
	
	public void createDirectory(String directoryName){
	 File directory = new File(directoryName);
	    if (! directory.exists()){
	        directory.mkdir();
	        Log.info(directoryName +" didn't exists so create it.");
	        // If you require it to make the entire directory path including parents,
	        // use directory.mkdirs(); here instead.
	    }
	    else{
	    	Log.info(directoryName +" already exists.");
	    }
	
	}
	
	public static String getFeatureFileName(Scenario scenario) {
	    String featureName = "Feature ";
	    String rawFeatureName = scenario.getId().split(";")[0].replace("-"," ");
	    featureName = featureName + rawFeatureName.substring(0, 1).toUpperCase() + rawFeatureName.substring(1);

	    return featureName;
	}
	
	/**
	 * description - Method to get random string
	 * @param count - String count
	 * @param letters - Boolean value pass either True/false to return string with alphabets
	 * @param numbers - Boolean value pass either True/false to return string with digits
	 * @return - random string
	 */
	public static String getrandomStringlatest(int count, boolean letters, boolean numbers) {
	    String generatedString = RandomStringUtils.random(count,letters,numbers);
	    return generatedString;
	}
	
	/**
	 * Description - Method to get random email
	 * @param env - Environment name
	 * @param prefix
	 * @return - random email
	 */
	
	public static String getEmail(String env, String prefix){
		return prefix +"_"+ env + "_" + getrandomStringlatest(6,true,true)+ "@gmail.com";
	}
	
	/**
	 * Description - Method to format the given date
	 * @param givenDate - Given in the application
	 * @param currentDateFormat
	 * @param desiredDateFormat
	 * @return
	 */
	
	public static String dateFormatChanger(String givenDate, String currentDateFormat, String desiredDateFormat) {
		String dateDesired="";
		try{
			DateFormat sdf = new SimpleDateFormat(currentDateFormat);
			Date date = sdf.parse(givenDate);
			dateDesired = new SimpleDateFormat(desiredDateFormat).format(date);
		}catch( ParseException e){
			Log.error("Exception while parsing the date!",e);
		}
		return dateDesired;
		}
	
	

		   
		  public static void zipFolderStructure(String sourceFolder, String zipFolder){
		    // Creating a ZipOutputStream by wrapping a FileOutputStream
		    try (FileOutputStream fos = new FileOutputStream(zipFolder); 
		         ZipOutputStream zos = new ZipOutputStream(fos)) {
		      Path sourcePath = Paths.get(sourceFolder);
		      // Walk the tree structure using WalkFileTree method
		      java.nio.file.Files.walkFileTree(sourcePath, new SimpleFileVisitor<Path>(){
		        @Override
		        // Before visiting the directory create the directory in zip archive
		        public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
		          // Don't create dir for root folder as it is already created with .zip name 
		          if(!sourcePath.equals(dir)){
		            System.out.println("Directory- " + dir);
		            zos.putNextEntry(new ZipEntry(sourcePath.relativize(dir).toString() + "/"));                  
		            zos.closeEntry();    
		          }
		          return FileVisitResult.CONTINUE;
		        }
		        @Override
		        // For each visited file add it to zip entry
		        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
		          System.out.println("File Name- " + sourcePath.relativize(file).toString());
		          zos.putNextEntry(new ZipEntry(sourcePath.relativize(file).toString()));
		          Files.copy(file, zos);
		          zos.closeEntry();
		          return FileVisitResult.CONTINUE;
		        }});
		    } catch (IOException e) {
		      // TODO Auto-generated catch block
		      e.printStackTrace();
		    }

	    }	

		
	
}