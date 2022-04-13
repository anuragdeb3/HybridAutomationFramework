package com.commonutils;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.imageio.ImageIO;

import org.apache.logging.log4j.LogManager;
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
import org.openqa.selenium.support.ui.Wait;
import org.openqa.selenium.support.ui.WebDriverWait;

import com.cucumber.listener.Reporter;
import com.google.common.io.Files;

import ru.yandex.qatools.ashot.AShot;
import ru.yandex.qatools.ashot.Screenshot;
import ru.yandex.qatools.ashot.shooting.ShootingStrategies;

public class WebUtils extends BasePage {
	
	private static final org.apache.logging.log4j.Logger Log = LogManager
			.getLogger(MethodHandles.lookup().lookupClass());
	public static SoftAssertions softAssert = new SoftAssertions();
	public static Properties prop = new Properties();
	
	
	// locator to element
	public WebElement getElement(By locator) {
		WaitUtils.waitAndFindElement(locator, 30);
		WebElement element = driver.findElement(locator);
		return (element);
	}

	// locator to elements
	public List<WebElement> getElements(By locator) {
		WaitUtils.waitForElement(driver, locator, 30);
		List<WebElement> elements = driver.findElements(locator);
		return (elements);
	}

	// get text from element
	public String getTextFromElement(By locator) {
		WaitUtils.waitAndFindElement(locator, 30);
		return getElement(locator).getText();
	}

	// get size from element
	public int getElementsSize(By locator) {
		WaitUtils.waitForElement(driver, locator, 30);
		return driver.findElements(locator).size();

	}
	
	public static void sendKeys(WebElement element, int timeout, String value) {
		new WebDriverWait(driver, timeout).until(ExpectedConditions.visibilityOf(element));
		element.sendKeys(value);
	}

	public void enterTextInInputwithDelete(By locator, String value) {
		try {
			Log.info("Initial Value *************enterTextInInputwithDelete********************** " + value);
			WaitUtils.waitAndFindElement(locator, 30);

			sendKeys(getElement(locator), 10, Keys.CONTROL + "a" + Keys.DELETE);
			sendKeys(getElement(locator), 10, value);
			sendKeys(getElement(locator), 10, Keys.TAB + "");

			Log.info("Final Value *************enterTextInInputwithDelete********************** "
					+ getElement(locator).getAttribute("value"));

		} catch (Exception e) {
			CommonUtils.captureScreenshot();
			Log.error("Exception :"+e);
		}
	}
	
	public  void clickOnElementwithWait(By locator, int timeInSec) {
		FluentWait<WebDriver> wait = null;
		
		//Log.info("Wait for Element started :" + element);
		try {
			wait = new FluentWait<WebDriver>(driver)
					.withTimeout(timeInSec, TimeUnit.SECONDS)
					.pollingEvery(1000, TimeUnit.MILLISECONDS)
					.withMessage("Timeout occured!")
					.ignoring(java.util.NoSuchElementException.class);
			
				
				wait.until(ExpectedConditions.visibilityOfElementLocated(locator));
				wait.until(ExpectedConditions.elementToBeClickable(locator));
				
			
			//CommonUtils.captureScreenshot();
			
			driver.findElement(locator).click();
			Log.info("clicked on :" + locator);

		} catch (Exception e) {
			CommonUtils.captureScreenshot();
			Log.error(e);

			// Assert.fail();
		}
	}
	
	public static boolean isElementPresent(By locator) {
		boolean status = false;

		try {
			status = driver.findElement(locator).isDisplayed();
			status = true;
			
			Log.info("Element is present");
		} catch (NoSuchElementException e) {
			status = false;
			Log.error("Element is not present ->"+ e.getMessage());
			
		} catch (Exception e) {
			status = false;
			Log.error("Element is not present ->"+ e.getMessage());

			// return status;
		}
		return status;
	}

	public void scrollToElement(By locatorwithElement) {

		Log.info("Scroll to "+locatorwithElement);
		WebElement ele;
			ele = driver.findElement(locatorwithElement);
			JavascriptExecutor jse = (JavascriptExecutor) driver;
			jse.executeScript("arguments[0].scrollIntoView(true);", ele);
	}
	
	public void clickTabKey(By locator) {

			driver.findElement(locator).sendKeys(Keys.TAB);
		
	}
	
	public void clickElementJS(By locatorwithElement) {

		WebElement ele = driver.findElement(locatorwithElement);
			JavascriptExecutor jse = (JavascriptExecutor) driver;
			jse.executeScript("arguments[0].click;", ele);

		}
	
	
	public boolean isElementSelected(By locator) {
		boolean status = false;

		try {
			status = driver.findElement(locator).isSelected();
			status = true;
			
			Log.info("Element is selected");
		} catch (NoSuchElementException e) {
			status = false;
			Log.error("Element is not selected "+ e);
			;
		} catch (Exception e) {
			status = false;
			Log.error("Element is not selected "+ e);

			// return status;
		}
		return status;
	}
	
	
	public boolean isElementVisible(By locator) {
		boolean status = false;

		try {
			status = driver.findElement(locator).isDisplayed();
			status = true;
			
			Log.info("Element is visible");
		} catch (NoSuchElementException e) {
			status = false;
			Log.error("Element is not visible "+ e);
			;
		} catch (Exception e) {
			status = false;
			Log.error("Element is not visible "+ e);

			// return status;
		}
		return status;
	}
	
	public static boolean isAttribtuePresent(WebElement element, String attribute) {
	    Boolean result = false;
	    try {
	        String value = element.getAttribute(attribute);
	        if (value != null){
	            result = true;
	        }
	    } catch (Exception e) {
	    	Log.error(attribute+" : Attribute is not present");
	    }

	    return result;
	}
	
	public void enterTextInInputwithDeleteUsingAction(By locator, String value) {
		try {
			Log.info("Initial Value *************enterTextInInputwithDelete********************** " + value);
			WaitUtils.waitForElement(driver,locator,30);
			WebElement ele = null;
		    ele=driver.findElement(locator);
			Actions actions = new Actions(driver);
			actions.doubleClick(ele).perform();
			WaitUtils.wait.until(ExpectedConditions.visibilityOf(ele)).sendKeys(Keys.DELETE);

			WaitUtils.wait.until(ExpectedConditions.visibilityOf(ele)).sendKeys(value);
			//Thread.sleep(900);
			WaitUtils.wait.until(ExpectedConditions.visibilityOf(ele)).sendKeys(Keys.TAB);
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
	public String getAttributeFromElement(By locator) {
		WaitUtils.waitAndFindElement(locator, 30);
		return getElement(locator).getAttribute("value");
	}
	
	public String getTextElementJS(By locator) {
		
		comUtils.scrollUp1(locator);

		 WebElement ele;

		 ele = driver.findElement(locator);
		 
		JavascriptExecutor jse = (JavascriptExecutor)driver;
		 String TexttoReturn=jse.executeScript(" return arguments[0].innerText;", ele).toString();
		// Log.info(TexttoReturn);

		 return TexttoReturn;
		 }
	 
		public void movetoelement() {
		    	String locator = "//h1";
		        try {
		            waitForLoading();
		            JavascriptExecutor jse = (JavascriptExecutor) driver;
		            //WaitUtils.waitAndFindElement(driver, locator, 120, Constants.xpath);
		            WebElement element = driver.findElement(By.xpath(locator));
		            jse.executeScript("arguments[0].scrollIntoView();", element);

		        } catch (Exception e) {
		            // TODO: handle exception
		            e.printStackTrace();
		        } 
		}
		 

	  
	public boolean checkUncheckCheckBox(By  locator) {
	    boolean status=false;   
		//if (value.equalsIgnoreCase("yes") || value.equalsIgnoreCase("check") || value.equalsIgnoreCase("Y")) {
	        	
	        	
	            if (!getElement(locator).isSelected()) {
	                movetoelement();
	                Log.info("not checked");
	                status=false; 
	                //clickOnElementwithWait(locator,20);
	            }
	         else if (getElement(locator).isSelected()) {
	            movetoelement();
	            Log.info("checked");
	            status=true; 
	            //clickOnElementwithWait( locator,20);
	        }
		//}
		return status;
	    } 
	 
	public void assertValidationPdf(boolean assertStatus , String message) {
		if (assertStatus == true) {
			softAssert.assertThat(true);
			Log.info("Pass " + message);
		} else {
			Log.info("Fail " + message);
			softAssert.fail("Assertion failure");
		}
		
	}

	public void scrollToTop() {

		JavascriptExecutor js = (JavascriptExecutor) driver;

		js.executeScript("window.scrollTo(0, document.body.scrollHeight)");
	}
	
	public static String getValueFromElement(By element) {
		String getText = null;
		WaitUtils.waitUntilElementIsVisible1(10,element);
		getText = driver.findElement(element).getAttribute("value");
		return getText;
		}
	
	public static String getTextElement(By element) {
		String getText = null;
		WaitUtils.waitUntilElementIsVisible1(10,element);
		getText = driver.findElement(element).getText();
		return getText;
		}
	
	public static void markTestStatusBS(String status, String reason, WebDriver driver) {  
		// the same WebDriver instance should be passed that is being used to run the test in the calling function
		JavascriptExecutor jse = (JavascriptExecutor)driver;
		jse.executeScript("browserstack_executor: {\"action\": \"setSessionStatus\", \"arguments\": {\"status\": \""+status+"\", \"reason\": \""+reason+"\"}}");
	}
	
	public static void failMarkOnBS(String errorMsg){
		WebUtils.markTestStatusBS("Failed", "Got Some Error : "+errorMsg, driver);
	}
	
	public static void passMarkOnBS(String passMsg){
		WebUtils.markTestStatusBS("Passed", passMsg, driver);
	}
	
	public static By elementByLabelText(String labelText){
    	
    	return By.xpath("//label[text()='"+labelText+"']");
    }
	
	public static void handlePopUp(){

		//String username =CommonUtils.getVal("proxyUser");
		//String password = CommonUtils.getVal("proxyUser");


		JavascriptExecutor jse = (JavascriptExecutor)driver;
		jse.executeScript("browserstack_executor: {\"action\": \"sendBasicAuth\", \"arguments\": {\"username\":\"debad\", \"password\": \"Tenacity!9\", \"timeout\": \"<time in milliseconds>\"}}");



		}
	/**
	 * description - Method to get inner text
	 * @param locator - x-path
	 * @param timeOut
	 * @return- inner text else null
	 */
	public String getAttributeInnerText(By locator, int timeOut) {
		try{
			WaitUtils.waitAndFindElement(locator, timeOut);
			return getElement(locator).getAttribute("innerText");
		}catch(Exception e){
			Log.error("Exception occured while reading xpath : {} ",locator.toString(),e);
		}
		return null;
	}
	
	/**
	 * description - Method to locate element by link text
	 * @param linkText - value
	 * @return- x-path
	 */
	public static By elementByLinkText(String linkText){
    	return By.xpath("//*[text()='"+linkText+"']");
    }

	/**
	 * This method has been written to click the button with button text name
	 */

	public static By elementByButtonText(String buttonText) {

		
		return By.xpath("//button[text()='" + buttonText + "']");
	

	}
	
	
}
