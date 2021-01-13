package com.anuragdeb.helper;

import io.github.bonigarcia.wdm.WebDriverManager;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.edge.EdgeDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.ie.InternetExplorerDriver;
import org.testng.Assert;

import com.anuragdeb.helper.Library;

public class BaseFactory {
	
	public static WebDriver driver;
	
	public static void browserFactory() {
	
	try {
	Library.LoadConfig("Config/Config.properties");
	
	String browserName = Library.getVal("browser");
	String browserVersion = Library.getVal("browserVersion");
	System.out.println("Browser Selected is : "+browserName);
	
	if (Library.isNullOrEmptyString(browserVersion)) {
			
			if (driver != null) {
				driver.quit();
			}
			
			if (browserName.equalsIgnoreCase("chrome")){
		
				WebDriverManager.chromedriver().setup();
				driver = new ChromeDriver();
				
				
			}else if(browserName.equalsIgnoreCase("firefox")){
			
				WebDriverManager.firefoxdriver().setup();
				driver = new FirefoxDriver();	
			}else if(browserName.equalsIgnoreCase("edge")){
			
				WebDriverManager.edgedriver().setup();
				driver = new EdgeDriver();	
			}else if(browserName.equalsIgnoreCase("ie")){
			
				WebDriverManager.iedriver().setup();
				driver = new InternetExplorerDriver();	
			}
			else {
				System.out.println("Sorry We don't support "+browserName +" Supported Names : chrome,firefox, edge,ie");
				Assert.fail("Sorry We don't support "+browserName +" Supported Names : chrome,firefox, edge,ie");
			}
			driver.manage().window().maximize();
	
	}
	else {	
			if (driver != null) {
				driver.quit();
			}
		
			if (browserName.equalsIgnoreCase("chrome")){
		
				WebDriverManager.chromedriver().browserVersion(browserVersion).setup();
				driver = new ChromeDriver();	
			}else if(browserName.equalsIgnoreCase("firefox")){
			
				WebDriverManager.firefoxdriver().browserVersion(browserVersion).setup();
				driver = new FirefoxDriver();	
			}else if(browserName.equalsIgnoreCase("edge")){
			
				WebDriverManager.edgedriver().browserVersion(browserVersion).setup();
				driver = new EdgeDriver();	
			}else if(browserName.equalsIgnoreCase("ie")){
			
				WebDriverManager.iedriver().browserVersion(browserVersion).setup();
				driver = new InternetExplorerDriver();	
			}
			else {
				System.out.println("Sorry We don't support "+browserName +" Supported Names : chrome,firefox, edge,ie");
			
			}
		}
	}catch(Exception e) {
		System.out.println("Exception captured at browserFactory method");
	}
	
	
	
	
	}
	public void tearDown() {
		
		driver.quit();
	}

}
