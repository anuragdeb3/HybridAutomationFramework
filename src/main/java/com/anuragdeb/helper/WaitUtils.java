package com.commonutils;

import com.google.common.base.Function;
import com.commonutils.BasePage;
import org.apache.logging.log4j.LogManager;
import org.openqa.selenium.*;
import org.openqa.selenium.support.ui.ExpectedCondition;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.FluentWait;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class WaitUtils extends BasePage {
	private static final org.apache.logging.log4j.Logger Log = LogManager
			.getLogger(MethodHandles.lookup().lookupClass());
	LombardUtils lomUtils = new LombardUtils();

	/** Default wait time for an element. 7 seconds. */
	public static final int DEFAULT_WAIT_4_ELEMENT = 7;
	/**
	 * Default wait time for a page to be displayed. 12 seconds. The average
	 * webpage load time is 6 seconds in 2012. Based on your tests, please set
	 * this value. "0" will nullify implicitlyWait and speed up a test.
	 */
	public static final int DEFAULT_WAIT_4_PAGE = 30;

	private static boolean _isVisible = true;
	public static FluentWait<WebDriver> wait;

	/*
	 * Wait for the element to be present in the DOM, and displayed on the page.
	 * And returns the first WebElement using the given method.
	 * 
	 * @param WebDriver The driver object to be used
	 * 
	 * @param By selector to find the element
	 * 
	 * @param int The time in seconds to wait until returning a failure
	 *
	 * @return WebElement the first WebElement using the given method, or null
	 * (if the timeout is reached)
	 */
	public static WebElement waitForElement(WebDriver driver, final By by, int timeOutInSeconds) {

		WebElement element;
		try {
			// To use WebDriverWait(), we would have to nullify
			// implicitlyWait().
			// Because implicitlyWait time also set "driver.findElement()" wait
			// time.
			// info from:
			// https://groups.google.com/forum/?fromgroups=#!topic/selenium-users/6VO_7IXylgY
			driver.manage().timeouts().implicitlyWait(1, TimeUnit.SECONDS); // nullify
																			// implicitlyWait()

			WebDriverWait wait = new WebDriverWait(driver, timeOutInSeconds);
			element = wait.until(ExpectedConditions.presenceOfElementLocated(by));

			if (_isVisible) {
				element = wait.until(ExpectedConditions.visibilityOfElementLocated(by));
				_isVisible = true;
			}

			driver.manage().timeouts().implicitlyWait(DEFAULT_WAIT_4_PAGE, TimeUnit.SECONDS); // reset
																								// implicitlyWait
			return element; // return the element
		} catch (Exception e) {
			e.printStackTrace();
			return null;

		}
	}

	public static boolean waitForEitherOfElements(WebDriver driver, final By by1, final By by2, int timeOutInSeconds) {
		boolean isThere = false;
		try {
			// To use WebDriverWait(), we would have to nullify
			// implicitlyWait().
			// Because implicitlyWait time also set "driver.findElement()" wait
			// time.
			// info from:
			// https://groups.google.com/forum/?fromgroups=#!topic/selenium-users/6VO_7IXylgY
			driver.manage().timeouts().implicitlyWait(1, TimeUnit.SECONDS); // nullify
																			// implicitlyWait()

			WebDriverWait wait = new WebDriverWait(driver, timeOutInSeconds);

			isThere = wait.until(ExpectedConditions.or(ExpectedConditions.presenceOfElementLocated(by1),
					ExpectedConditions.presenceOfElementLocated(by2)));

			driver.manage().timeouts().implicitlyWait(DEFAULT_WAIT_4_PAGE, TimeUnit.SECONDS); // reset
																								// implicitlyWait
		} catch (Exception e) {
			e.printStackTrace();

		}
		return isThere;
	}

	//
	// public static WebElement waitForElements(WebDriver driver, final By by,
	// int timeOutInSeconds) {
	//
	// List<WebElement> elements = null;;
	// try{
	// //To use WebDriverWait(), we would have to nullify implicitlyWait().
	// //Because implicitlyWait time also set "driver.findElement()" wait time.
	// //info from:
	// https://groups.google.com/forum/?fromgroups=#!topic/selenium-users/6VO_7IXylgY
	// driver.manage().timeouts().implicitlyWait(1, TimeUnit.SECONDS); //nullify
	// implicitlyWait()
	//
	// WebDriverWait wait = new WebDriverWait(driver, timeOutInSeconds);
	// elements =
	// wait.until(ExpectedConditions.presenceOfAllElementsLocatedBy(by));
	//
	// if (_isVisible ) {
	// elements = wait.until(ExpectedConditions.(elements));
	// _isVisible = true;
	// }
	//
	// driver.manage().timeouts().implicitlyWait(DEFAULT_WAIT_4_PAGE,
	// TimeUnit.SECONDS); //reset implicitlyWait
	// return elements; //return the element
	// } catch (Exception e) {
	// e.printStackTrace();
	// return null;
	//
	// }
	// }

	public static WebElement waitForElement(WebDriver driver, final By by, int timeOutInSeconds, boolean isVisible) {
		_isVisible = isVisible;
		return waitForElement(driver, by, timeOutInSeconds);
	}

	/*
	 * Wait for the element to be present in the DOM, regardless of being
	 * displayed or not. And returns the first WebElement using the given
	 * method.
	 *
	 * @param WebDriver The driver object to be used
	 * 
	 * @param By selector to find the element
	 * 
	 * @param int The time in seconds to wait until returning a failure
	 * 
	 * @return WebElement the first WebElement using the given method, or null
	 * (if the timeout is reached)
	 */
	public static WebElement waitForElementPresent(WebDriver driver, final By by, int timeOutInSeconds) {
		WebElement element;
		try {
			driver.manage().timeouts().implicitlyWait(0, TimeUnit.SECONDS); // nullify
																			// implicitlyWait()

			WebDriverWait wait = new WebDriverWait(driver, timeOutInSeconds);
			element = wait.until(ExpectedConditions.presenceOfElementLocated(by));
			element = wait.until(ExpectedConditions.visibilityOfElementLocated(by));

			driver.manage().timeouts().implicitlyWait(DEFAULT_WAIT_4_PAGE, TimeUnit.SECONDS); // reset
																								// implicitlyWait
			return element; // return the element
		} catch (Exception e) {
			// e.printStackTrace();
		}
		return null;
	}

	/*
	 * Wait for the element to be present in the DOM, regardless of being
	 * displayed or not. And returns the first WebElement using the given
	 * method.
	 *
	 * @param WebDriver The driver object to be used
	 * 
	 * @param By selector to find the element
	 * 
	 * @param int The time in seconds to wait until returning a failure
	 *
	 * @return WebElement the first WebElement using the given method, or null
	 * (if the timeout is reached)
	 */
	public static WebElement waitForElementPresentAndClickAble(WebDriver driver, final By by, int timeOutInSeconds) {
		WebElement element;
		try {
			driver.manage().timeouts().implicitlyWait(0, TimeUnit.SECONDS); // nullify
																			// implicitlyWait()

			WebDriverWait wait = new WebDriverWait(driver, timeOutInSeconds);
			element = wait.until(ExpectedConditions.presenceOfElementLocated(by));
			element = wait.until(ExpectedConditions.visibilityOfElementLocated(by));
			element = wait.until(ExpectedConditions.elementToBeClickable(by));

			driver.manage().timeouts().implicitlyWait(DEFAULT_WAIT_4_PAGE, TimeUnit.SECONDS); // reset
																								// implicitlyWait
			return element; // return the element
		} catch (Exception e) {
			// e.printStackTrace();
		}
		return null;
	}

	/*
	 * Wait for the List<WebElement> to be present in the DOM, regardless of
	 * being displayed or not. Returns all elements within the current page DOM.
	 * 
	 * @param WebDriver The driver object to be used
	 * 
	 * @param By selector to find the element
	 * 
	 * @param int The time in seconds to wait until returning a failure
	 *
	 * @return List<WebElement> all elements within the current page DOM, or
	 * null (if the timeout is reached)
	 */
	public static List<WebElement> waitForListElementsPresent(WebDriver driver, final By by, int timeOutInSeconds) {
		List<WebElement> elements;
		try {
			driver.manage().timeouts().implicitlyWait(0, TimeUnit.SECONDS); // nullify
																			// implicitlyWait()

			WebDriverWait wait = new WebDriverWait(driver, timeOutInSeconds);
			wait.until((new ExpectedCondition<Boolean>() {
				@Override
				public Boolean apply(WebDriver driverObject) {
					return areElementsPresent(driverObject, by);
				}
			}));

			elements = driver.findElements(by);
			driver.manage().timeouts().implicitlyWait(DEFAULT_WAIT_4_PAGE, TimeUnit.SECONDS); // reset
																								// implicitlyWait
			return elements; // return the element
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/*
	 * Wait for an element to appear on the refreshed web-page. And returns the
	 * first WebElement using the given method.
	 *
	 * This method is to deal with dynamic pages.
	 * 
	 * Some sites I (Mark) have tested have required a page refresh to add
	 * additional elements to the DOM. Generally you (Chon) wouldn't need to do
	 * this in a typical AJAX scenario.
	 * 
	 * @param WebDriver The driver object to use to perform this element search
	 * 
	 * @param locator selector to find the element
	 * 
	 * @param int The time in seconds to wait until returning a failure
	 * 
	 * @return WebElement the first WebElement using the given method, or
	 * null(if the timeout is reached)
	 * 
	 * @author Mark Collin
	 */
	public static WebElement waitForElementRefresh(WebDriver driver, final By by, int timeOutInSeconds) {
		WebElement element;
		try {
			driver.manage().timeouts().implicitlyWait(0, TimeUnit.SECONDS); // nullify
																			// implicitlyWait()
			new WebDriverWait(driver, timeOutInSeconds) {
			}.until(new ExpectedCondition<Boolean>() {

				@Override
				public Boolean apply(WebDriver driverObject) {
					driverObject.navigate().refresh(); // refresh the page
														// ****************
					return isElementPresentAndDisplay(driverObject, by);
				}
			});
			element = driver.findElement(by);
			driver.manage().timeouts().implicitlyWait(DEFAULT_WAIT_4_PAGE, TimeUnit.SECONDS); // reset
																								// implicitlyWait
			return element; // return the element
		} catch (Exception e) {
			driver.manage().timeouts().implicitlyWait(DEFAULT_WAIT_4_PAGE, TimeUnit.SECONDS);
			e.printStackTrace();

		}
		return null;
	}

	/*
	 * Wait for the Text to be present in the given element, regardless of being
	 * displayed or not.
	 *
	 * @param WebDriver The driver object to be used to wait and find the
	 * element
	 * 
	 * @param locator selector of the given element, which should contain the
	 * text
	 * 
	 * @param String The text we are looking
	 * 
	 * @param int The time in seconds to wait until returning a failure
	 * 
	 * @return boolean
	 */
	public static boolean waitForTextPresent(WebDriver driver, final By by, final String text, int timeOutInSeconds) {
		boolean isPresent = false;
		try {
			driver.manage().timeouts().implicitlyWait(0, TimeUnit.SECONDS); // nullify
																			// implicitlyWait()
			new WebDriverWait(driver, timeOutInSeconds) {
			}.until(new ExpectedCondition<Boolean>() {

				@Override
				public Boolean apply(WebDriver driverObject) {
					return isTextPresent(driverObject, by, text); // is the Text
																	// in the
																	// DOM
				}
			});
			isPresent = isTextPresent(driver, by, text);
			driver.manage().timeouts().implicitlyWait(DEFAULT_WAIT_4_PAGE, TimeUnit.SECONDS); // reset
																								// implicitlyWait
			return isPresent;
		} catch (Exception e) {
			driver.manage().timeouts().implicitlyWait(DEFAULT_WAIT_4_PAGE, TimeUnit.SECONDS);
			e.printStackTrace();
		}
		return false;
	}

	/*
	 * Waits for the Condition of JavaScript.
	 *
	 *
	 * @param WebDriver The driver object to be used to wait and find the
	 * element
	 * 
	 * @param String The javaScript condition we are waiting. e.g.
	 * "return (xmlhttp.readyState >= 2 && xmlhttp.status == 200)"
	 * 
	 * @param int The time in seconds to wait until returning a failure
	 * 
	 * @return boolean true or false(condition fail, or if the timeout is
	 * reached)
	 **/
	public static boolean waitForJavaScriptCondition(WebDriver driver, final String javaScript, int timeOutInSeconds) {
		boolean jscondition = false;
		try {
			driver.manage().timeouts().implicitlyWait(0, TimeUnit.SECONDS); // nullify
																			// implicitlyWait()
			new WebDriverWait(driver, timeOutInSeconds) {
			}.until(new ExpectedCondition<Boolean>() {

				@Override
				public Boolean apply(WebDriver driverObject) {
					return (Boolean) ((JavascriptExecutor) driverObject).executeScript(javaScript);
				}
			});
			jscondition = (Boolean) ((JavascriptExecutor) driver).executeScript(javaScript);
			driver.manage().timeouts().implicitlyWait(DEFAULT_WAIT_4_PAGE, TimeUnit.SECONDS); // reset
																								// implicitlyWait
			return jscondition;
		} catch (Exception e) {
			driver.manage().timeouts().implicitlyWait(DEFAULT_WAIT_4_PAGE, TimeUnit.SECONDS);
			e.printStackTrace();
		}
		return false;
	}

	/*
	 * Waits for the completion of Ajax jQuery processing by checking
	 * "return jQuery.active == 0" condition.
	 *
	 * @param WebDriver - The driver object to be used to wait and find the
	 * element
	 * 
	 * @param int - The time in seconds to wait until returning a failure
	 * 
	 * @return boolean true or false(condition fail, or if the timeout is
	 * reached)
	 */
	public static boolean waitForJQueryProcessing(WebDriver driver, int timeOutInSeconds) {
		boolean jQcondition = false;
		try {
			driver.manage().timeouts().implicitlyWait(0, TimeUnit.SECONDS); // nullify
																			// implicitlyWait()
			new WebDriverWait(driver, timeOutInSeconds) {
			}.until(new ExpectedCondition<Boolean>() {

				@Override
				public Boolean apply(WebDriver driverObject) {
					long jQryState = (long) ((JavascriptExecutor) driverObject).executeScript("return jQuery.active");
					Log.info("jQuery is currently active" + jQryState);
					return (Boolean) ((JavascriptExecutor) driverObject).executeScript("return jQuery.active == 0");

				}
			});
			jQcondition = (Boolean) ((JavascriptExecutor) driver).executeScript("return jQuery.active == 0");
			Log.info("jQuery is currently active" + jQcondition);
			driver.manage().timeouts().implicitlyWait(DEFAULT_WAIT_4_PAGE, TimeUnit.SECONDS); // reset
																								// implicitlyWait
			return jQcondition;
		} catch (Exception e) {
			driver.manage().timeouts().implicitlyWait(DEFAULT_WAIT_4_PAGE, TimeUnit.SECONDS);
			e.printStackTrace();
		}
		return jQcondition;
	}

	/**
	 * Coming to implicit wait, If you have set it once then you would have to
	 * explicitly set it to zero to nullify it -
	 */
	public static void nullifyImplicitWait(WebDriver driver) {
		driver.manage().timeouts().implicitlyWait(0, TimeUnit.SECONDS); // nullify
																		// implicitlyWait()
	}

	/**
	 * Set driver implicitlyWait() time.
	 */
	public static void setImplicitWait(WebDriver driver, int waitTime_InSeconds) {
		driver.manage().timeouts().implicitlyWait(waitTime_InSeconds, TimeUnit.SECONDS);
	}

	/**
	 * Reset ImplicitWait. To reset ImplicitWait time you would have to
	 * explicitly set it to zero to nullify it before setting it with a new time
	 * value.
	 */
	public static void resetImplicitWait(WebDriver driver) {
		driver.manage().timeouts().implicitlyWait(0, TimeUnit.SECONDS); // nullify
																		// implicitlyWait()
		driver.manage().timeouts().implicitlyWait(DEFAULT_WAIT_4_PAGE, TimeUnit.SECONDS); // reset
																							// implicitlyWait
	}

	/*
	 * Reset ImplicitWait.
	 * 
	 * @param int - a new wait time in seconds
	 */
	public static void resetImplicitWait(WebDriver driver, int newWaittime_InSeconds) {
		driver.manage().timeouts().implicitlyWait(0, TimeUnit.SECONDS); // nullify
																		// implicitlyWait()
		driver.manage().timeouts().implicitlyWait(newWaittime_InSeconds, TimeUnit.SECONDS); // reset
																							// implicitlyWait
	}

	/**
	 * Checks if the text is present in the element.
	 * 
	 * @param driver
	 *            - The driver object to use to perform this element search
	 * @param by
	 *            - selector to find the element that should contain text
	 * @param text
	 *            - The Text element you are looking for
	 * @return true or false
	 */
	private static boolean isTextPresent(WebDriver driver, By by, String text) {
		try {
			return driver.findElement(by).getText().contains(text);
		} catch (NullPointerException e) {
			return false;
		}
	}

	/**
	 * Checks if the elment is in the DOM, regardless of being displayed or not.
	 * 
	 * @param driver
	 *            - The driver object to use to perform this element search
	 * @param by
	 *            - selector to find the element
	 * @return boolean
	 */
	@SuppressWarnings("unused")
	private static boolean isElementPresent(WebDriver driver, By by) {
		try {
			driver.findElement(by);// if it does not find the element throw
									// NoSuchElementException, which calls
									// "catch(Exception)" and returns false;
			return true;
		} catch (NoSuchElementException e) {
			return false;
		}
	}

	/**
	 * Checks if the List<WebElement> are in the DOM, regardless of being
	 * displayed or not.
	 * 
	 * @param driver
	 *            - The driver object to use to perform this element search
	 * @param by
	 *            - selector to find the element
	 * @return boolean
	 */
	private static boolean areElementsPresent(WebDriver driver, By by) {
		try {
			driver.findElements(by);
			return true;
		} catch (NoSuchElementException e) {
			return false;
		}
	}

	/**
	 * Checks if the elment is in the DOM and displayed.
	 * 
	 * @param driver
	 *            - The driver object to use to perform this element search
	 * @param by
	 *            - selector to find the element
	 * @return boolean
	 */
	private static boolean isElementPresentAndDisplay(WebDriver driver, By by) {
		try {
			return driver.findElement(by).isDisplayed();
		} catch (NoSuchElementException e) {
			return false;
		}
	}

	public static void sleepTime(int seconds) {
		Log.info("-------  sleep for ------" + seconds + "     ----seconds---");

		int sleepTime = seconds * 1000;
		try {
			Thread.sleep(sleepTime);

		} catch (InterruptedException e) {
			
			Log.error("Exception : " + e);
			Thread.currentThread().interrupt();
		}
	}

	public WebElement waitForElementOne(String element, String locType) {
		WebElement webElement = null;
		Log.info("Waiting for Element " + element + " initiated");
		wait = new FluentWait<WebDriver>(driver).withTimeout(30, TimeUnit.SECONDS)
				.pollingEvery(1000, TimeUnit.MILLISECONDS).withMessage("Timeout occured!")
				.ignoring(java.util.NoSuchElementException.class);

		if (locType.equalsIgnoreCase("id")) {

			webElement = wait.until(new Function<WebDriver, WebElement>() {
				public WebElement apply(WebDriver driver) {
					return driver.findElement(By.id(element));
				}
			});
		} else if (locType.equalsIgnoreCase("xpath")) {
			webElement = wait.until(new Function<WebDriver, WebElement>() {
				public WebElement apply(WebDriver driver) {
					return driver.findElement(By.xpath(element));
				}
			});
		}
		Log.info("Waiting for Element " + element + " exited");

		return webElement;

	}

	public static void waitForElementWithTime(String element, String locType, int timeInSec) {

		Log.info("Waiting for Element " + element + " initiated");
		wait = new FluentWait<WebDriver>(driver).withTimeout(timeInSec, TimeUnit.SECONDS)
				.pollingEvery(1000, TimeUnit.MILLISECONDS).withMessage("Timeout occured!")
				.ignoring(java.util.NoSuchElementException.class);
		if (locType.equalsIgnoreCase("id")) {
			wait.until(ExpectedConditions.visibilityOfElementLocated(By.id(element)));
			wait.until(ExpectedConditions.presenceOfElementLocated(By.id(element)));
			driver.manage().timeouts().implicitlyWait(5, TimeUnit.SECONDS);
		} else if (locType.equalsIgnoreCase("xpath")) {
			wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(element)));
			wait.until(ExpectedConditions.elementToBeClickable(By.xpath(element)));
			driver.manage().timeouts().implicitlyWait(5, TimeUnit.SECONDS);
		}
		Log.info("Waiting for Element " + element + " exited");

	}

	public static void waitForElement(String element, String locType) {

		Log.info("Waiting for Element " + element + " initiated");
		wait = new FluentWait<WebDriver>(driver).withTimeout(30, TimeUnit.SECONDS)
				.pollingEvery(1000, TimeUnit.MILLISECONDS).withMessage("Timeout occured!")
				.ignoring(java.util.NoSuchElementException.class);
		if (locType.equalsIgnoreCase("id")) {
			wait.until(ExpectedConditions.visibilityOfElementLocated(By.id(element)));
			wait.until(ExpectedConditions.presenceOfElementLocated(By.id(element)));
			driver.manage().timeouts().implicitlyWait(5, TimeUnit.SECONDS);
		} else if (locType.equalsIgnoreCase("xpath")) {
			wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(element)));
			wait.until(ExpectedConditions.elementToBeClickable(By.xpath(element)));
			driver.manage().timeouts().implicitlyWait(5, TimeUnit.SECONDS);
		}
		Log.info("Waiting for Element " + element + " exited");

	}

	public static void waitForTextPresent(String element, String locType, String expText) {

		Log.info("Waiting for text " + expText);
		wait = new FluentWait<WebDriver>(driver).withTimeout(30, TimeUnit.SECONDS)
				.pollingEvery(1000, TimeUnit.MILLISECONDS).withMessage("Timeout occured!")
				.ignoring(java.util.NoSuchElementException.class);
		if (locType.equalsIgnoreCase("id")) {
			wait.until(ExpectedConditions.textToBePresentInElementValue(By.id(element), expText));
			driver.manage().timeouts().implicitlyWait(5, TimeUnit.SECONDS);
		} else if (locType.equalsIgnoreCase("xpath")) {
			wait.until(ExpectedConditions.textToBePresentInElementValue(By.xpath(element), expText));
			driver.manage().timeouts().implicitlyWait(5, TimeUnit.SECONDS);
		}

	}

	// Find element with FluentWait
	public static WebElement waitAndFindElement(final WebDriver driver, String element, final int timeoutSeconds,
			String locType) {
		// FluentWait Decleration
		FluentWait<WebDriver> wait = new FluentWait<WebDriver>(driver).withTimeout(timeoutSeconds, TimeUnit.SECONDS) // Set
																														// timeout
				.pollingEvery(100, TimeUnit.MILLISECONDS) // Set
															// query/check/control
															// interval
				.withMessage("Timeout occured!") // Set timeout message
				.ignoring(NoSuchElementException.class); // Ignore
															// NoSuchElementException

		// Wait until timeout period and when an element is found, then return
		// it.
		return wait.until(new Function<WebDriver, WebElement>() {
			WebElement webElement = null;

			public WebElement apply(WebDriver webDriver) {
				if (locType.equalsIgnoreCase("Id")) {
					webElement = driver.findElement(By.id(element));
				} else if (locType.equalsIgnoreCase("Xpath")) {
					webElement = driver.findElement(By.xpath(element));
				}
				return webElement;
			}
		});
	}

	public static void waitForEitherElement(int timeoutSeconds, String elementXpath1, String elementXpath2) {

		FluentWait<WebDriver> wait = new FluentWait<WebDriver>(driver).withTimeout(timeoutSeconds, TimeUnit.SECONDS) // Set
																														// timeout
				.pollingEvery(100, TimeUnit.MILLISECONDS) // Set
															// query/check/control
															// interval
				.withMessage("Timeout occured!") // Set timeout message
				.ignoring(NoSuchElementException.class); // Ignore
															// NoSuchElementException

		wait.until(ExpectedConditions.or(ExpectedConditions.presenceOfElementLocated(By.xpath(elementXpath1)),
				ExpectedConditions.presenceOfElementLocated(By.xpath(elementXpath2))));
	}

	public static void waitForPageLoad(int timeOut) {
		Log.info("Wait for Page Load Started. Duration :" + timeOut);
		new WebDriverWait(driver, timeOut).until(webDriver -> ((JavascriptExecutor) webDriver)
				.executeScript("return document.readyState").equals("complete"));
	}

	// Methods for webelemt waits
	public static void waitUntilElementIsVisible(long ito, String element) {
		WebElement webElement = driver.findElement(By.xpath(element));
		WebDriverWait wait = new WebDriverWait(driver, ito);
		wait.until(ExpectedConditions.visibilityOf(webElement));
	}

	// Methods for webelemt waits
	public static void waitUntilElementIsVisible(long ito, WebElement webElement) {

		WebDriverWait wait = new WebDriverWait(driver, ito);
		wait.until(ExpectedConditions.visibilityOf(webElement));
	}

	public static void waitUntilelementToBeClickable(WebDriver driver, long ito, WebElement webElement) {
		try {
			WebDriverWait wait = new WebDriverWait(driver, ito);
			wait.until(ExpectedConditions.elementToBeClickable(webElement));
		} catch (Exception e) {

			if (e.getMessage().contains("Expected condition failed: waiting for element to be clickable")) {

				ReporterA.reportFail("Expected condition failed: waiting for element to be clickable", "",
						e.getMessage());
			}

		}
	}

	public static void waitForInvisibilityOfElement(String element, String locType) {
		try {

			Log.info("Waiting for Element invisible" + element + " initiated");
			wait = new FluentWait<WebDriver>(driver).withTimeout(30, TimeUnit.SECONDS)
					.pollingEvery(1000, TimeUnit.MILLISECONDS).withMessage("Timeout occured!")
					.ignoring(java.util.NoSuchElementException.class);
			if (locType.equalsIgnoreCase("id")) {
				wait.until(ExpectedConditions.invisibilityOfElementLocated(By.id(element)));
			} else if (locType.equalsIgnoreCase("xpath")) {
				wait.until(ExpectedConditions.invisibilityOfElementLocated(By.xpath(element)));
			}
			Log.info("Waiting for Element invisible" + element + " exited");
		} catch (Exception e) {

			driver.manage().timeouts().implicitlyWait(30, TimeUnit.SECONDS);
			Log.info("Fluent Wait exception");
			e.printStackTrace();
		}

	}

	public void waitForElementToBeVisible(String element, String locType) {
		Log.info("Entering waitForElementToVisible");
		driver.manage().timeouts().implicitlyWait(0, TimeUnit.SECONDS);
		try {
			By by = lomUtils.returnByfromLocator(element, locType);
			WebDriverWait wait = new WebDriverWait(driver, 60);
			wait.until(ExpectedConditions.visibilityOfElementLocated(by));
			WaitUtils.sleepTime(1);
			driver.manage().timeouts().implicitlyWait(
					Integer.parseInt(GlobalVariable.executionProps.getProperty("DEFAULT_WAIT_4_PAGE_SEC")),
					TimeUnit.SECONDS);
		} catch (Exception e) {
			lomUtils.failCase(e);
		}
	}

	/*
	 * Author :debad
	 * 
	 * @param driver
	 * 
	 * @param ito
	 * 
	 * @param webElement
	 */

	public static void waitUntilelementToBeSelected(WebDriver driver, long ito, WebElement webElement) {
		WebDriverWait wait = new WebDriverWait(driver, ito);
		wait.until(ExpectedConditions.elementToBeSelected(webElement));
	}

	public static void waitUntiltitleIs(WebDriver driver, long ito, String eTitle) {
		WebDriverWait wait = new WebDriverWait(driver, ito);
		wait.until(ExpectedConditions.titleIs(eTitle));
	}

	public static void waitForGivenTimeInSeconds(int wTmeInSeconds) {
		try {
			TimeUnit.SECONDS.sleep(wTmeInSeconds);
		} catch (InterruptedException e) {
			Log.error("InterruptedException caught at  waitForGivenTimeInSeconds method", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		}
	}

	public static WebElement waitAndFindElement(String element, String locType, final int timeoutSeconds) {
		// FluentWait Decleration
		Log.info("Timeout Started, Waiting for Element: " + element);
		FluentWait<WebDriver> wait = new FluentWait<WebDriver>(driver).withTimeout(timeoutSeconds, TimeUnit.SECONDS) // Set
																														// timeout
				.pollingEvery(100, TimeUnit.MILLISECONDS) // Set
															// query/check/control
															// interval
				.withMessage("Timeout occured!") // Set timeout message
				.ignoring(NoSuchElementException.class); // Ignore
															// NoSuchElementException

		// Wait until timeout period and when an element is found, then return
		// it.
		return wait.until(new Function<WebDriver, WebElement>() {
			WebElement webElement = null;

			public WebElement apply(WebDriver webDriver) {
				if (locType.equalsIgnoreCase("Id")) {
					webElement = driver.findElement(By.id(element));
				} else if (locType.equalsIgnoreCase("Xpath")) {
					webElement = driver.findElement(By.xpath(element));
				}
				return webElement;
			}
		});
	}

	public static WebElement waitAndFindElement(WebElement element, final int timeoutSeconds) {
		// FluentWait Declaration
		FluentWait<WebDriver> wait = new FluentWait<WebDriver>(driver).withTimeout(timeoutSeconds, TimeUnit.SECONDS) // Set timeout
				.pollingEvery(100, TimeUnit.MILLISECONDS) // Set query/check/control interval
				.withMessage("Timeout occured!") // Set timeout message
				.ignoring(NoSuchElementException.class); // Ignore NoSuchElementException

		// Wait until timeout period and when an element is found, then return it.
		return wait.until(new Function<WebDriver, WebElement>() {
			WebElement webElement = null;

			public WebElement apply(WebDriver webDriver) {

				webElement = element;

				return webElement;
			}
		});
	}

	public static WebElement waitAndFindElement(By element, final int timeoutSeconds) {
		// FluentWait Declaration
		FluentWait<WebDriver> wait = new FluentWait<WebDriver>(driver)
				.withTimeout(timeoutSeconds, TimeUnit.SECONDS) // Set timeout
				.pollingEvery(1000, TimeUnit.MILLISECONDS) // Set query/check/control interval
				.withMessage("Timeout occured!") // Set timeout message
				.ignoring(NoSuchElementException.class); // Ignore NoSuchElementException

		// Wait until timeout period and when an element is found, then return it.
		return wait.until(new Function<WebDriver, WebElement>() {
			WebElement webElement = null;

			public WebElement apply(WebDriver webDriver) {
				webElement = driver.findElement(element);

				return webElement;
			}
		});
	}
	
	public static void waitUntilElementIsVisible1(long ito, By locator) {
	try {
		WebElement webElement = driver.findElement(locator);

		Log.info("Waiting for Element invisible" + locator + " initiated");
		wait = new FluentWait<WebDriver>(driver).withTimeout(ito, TimeUnit.SECONDS)
				.pollingEvery(1000, TimeUnit.MILLISECONDS).withMessage("Timeout occured!")
				.ignoring(java.util.NoSuchElementException.class);
		
		wait.until(ExpectedConditions.visibilityOf(webElement));
		Log.info("Waiting for Element invisible" + webElement + " exited");
	} catch (Exception e) {

		
		Log.error("Fluent Wait exception");
		e.printStackTrace();
	}

}
	
	
	public static void waitForElementToBeClickable(By elementLoc, int timeout) {
		
		try {
			
			wait = new FluentWait<WebDriver>(driver).withTimeout(timeout, TimeUnit.SECONDS)
					.pollingEvery(1000, TimeUnit.MILLISECONDS).withMessage("Timeout occured!")
					.ignoring(java.util.NoSuchElementException.class);
			
			
			wait.until(ExpectedConditions.visibilityOfElementLocated(elementLoc));
			wait.until(ExpectedConditions.presenceOfElementLocated(elementLoc));
			wait.until(ExpectedConditions.elementToBeClickable(elementLoc));
			
		} catch (Exception e) {
			
		}
	}
	
	
	public static void clickOnElementwithWait(By elementLoc, int timeInSec) {
		
		try {
			wait = new FluentWait<WebDriver>(driver)
					.withTimeout(timeInSec, TimeUnit.SECONDS)
					.pollingEvery(1000, TimeUnit.MILLISECONDS)
					.withMessage("Timeout occured!")
					.ignoring(java.util.NoSuchElementException.class);
			
			
				wait.until(ExpectedConditions.visibilityOfElementLocated(elementLoc));
				wait.until(ExpectedConditions.elementToBeClickable(elementLoc));
				
			
			
			
			driver.findElement(elementLoc).click();
			CommonUtils.captureScreenshot();
			

		} catch (Exception e) {
			CommonUtils.captureScreenshot();
			Log.error(e);
			// Assert.fail();
		}
	}
	
	
	public static void clickOnElementJSWait(By elementLoc, int timeInSec) {
	
	
	
	WebDriverWait wait = new WebDriverWait(driver, 20);
	WebElement element = wait.until(ExpectedConditions.elementToBeClickable(By.xpath("//span[@class='workflow-state' and text()='All Invoices Received']"))); 
	((JavascriptExecutor)driver).executeScript("arguments[0].click();", element);
	
	}
	
}


