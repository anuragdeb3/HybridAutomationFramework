package com.commonutils;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.openqa.selenium.Proxy;
import org.openqa.selenium.Proxy.ProxyType;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.ie.InternetExplorerDriver;
import org.openqa.selenium.remote.CapabilityType;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;



public class BrowserUtils {
	private static final org.apache.logging.log4j.Logger Log = LogManager
			.getLogger(MethodHandles.lookup().lookupClass());

	public static String CurrentBrowser = null;
	final static String PROXY = "userproxy.rbsgrp.net:8080";
	static DesiredCapabilities cap;
	private static ChromeOptions options = new ChromeOptions();
	public static String URL = "";

	public BrowserUtils() {

	}
	
	public static WebDriver getChromeDriverNoProxy() {

		String strChromeDriverPath=GenericUtils.getProperty("ChromeDriver");
		String strChromeBinaryPath=GenericUtils.getProperty("ChromeBinary");

		new File(GenericUtils.getProperty("DownloadPath")).mkdirs();
		String strDownloadPath=GenericUtils.getProperty("DownloadPath");
		Map<String, Object> prefs = new HashMap<String, Object>();
		prefs.put("download.default_directory",strDownloadPath);

		@SuppressWarnings("unused")
		String strChromeProfile=GenericUtils.getProperty("ChromeDefaultProfile");
		System.setProperty("webdriver.chrome.driver",strChromeDriverPath);
		
		Log.info("Setting options");
		// set chrome options to disable blank data page while opening

		options.setExperimentalOption("prefs", prefs);
		options.addArguments("test-type");
		options.addArguments("--start-maximized");
//      options.addArguments("user-data-dir=" + strChromeProfile);
		options.addArguments("chrome.switches","--disable-extensions");
		options.addArguments("--enable-javascript");
		options.addArguments("--disable-dev-shm-usage");


		//options.addArguments("--disable-infobars");
		//options.addArguments("--no-sandbox");
		options.addArguments("--remote-debugging-port=9222");

		//options.setExperimentalOption("useAutomationExtension", false);
		options.setBinary(strChromeBinaryPath);

/*
		 String prox = "nftproxy.lb1.rbsgrp.mde:8080";
	      // set browser settings with Desired Capabilities
	      Proxy proxy = new Proxy(); proxy.setHttpProxy(prox).setFtpProxy(prox).setSslProxy(prox)
	      .setSocksProxy(prox);
	      options.setCapability(CapabilityType.PROXY, proxy);
		
	*/	
		
		

		Proxy proxy= new Proxy();
//		proxy.setProxyType(ProxyType.DIRECT);
		proxy.setProxyType(ProxyType.MANUAL); 
		WebDriver driver;

		
		
		options.setCapability("proxy",proxy);
		 driver= new ChromeDriver(options);
		Log.info("Chrome Browser is launched with no proxy");
		driver.manage().window().maximize();
		return driver;
	}

	public static WebDriver getChromeDriverWithProxy() {

		String strChromeDriverPath=GenericUtils.getProperty("ChromeDriver");
		String strChromeBinaryPath=GenericUtils.getProperty("ChromeBinary");

		new File(GenericUtils.getProperty("DownloadPath")).mkdirs();
		String strDownloadPath=GenericUtils.getProperty("DownloadPath");
		Map<String, Object> prefs = new HashMap<String, Object>();
		prefs.put("download.default_directory",strDownloadPath);

		@SuppressWarnings("unused")
		String strChromeProfile=GenericUtils.getProperty("ChromeDefaultProfile");
		System.setProperty("webdriver.chrome.driver",strChromeDriverPath);
		
		Log.info("Setting options");
		// set chrome options to disable blank data page while opening

		options.setExperimentalOption("prefs", prefs);
		options.addArguments("test-type");
		options.addArguments("--start-maximized");
//      options.addArguments("user-data-dir=" + strChromeProfile);
		options.addArguments("chrome.switches","--disable-extensions");
		options.addArguments("--enable-javascript");
		options.addArguments("--disable-dev-shm-usage");


		//options.addArguments("--disable-infobars");
		//options.addArguments("--no-sandbox");
		options.addArguments("--remote-debugging-port=9222");

		//options.setExperimentalOption("useAutomationExtension", false);
		options.setBinary(strChromeBinaryPath);


		 String prox = CommonUtils.getVal("Proxy")+":8080";
		 Log.info("Proxy Set to "+CommonUtils.getVal("Proxy")+":8080");
	      // set browser settings with Desired Capabilities
	      Proxy proxy = new Proxy(); 
	      proxy.setHttpProxy(prox)
	      .setFtpProxy(prox)
	      .setSslProxy(prox)
	      .setSocksProxy(prox);

		WebDriver driver;
		options.addArguments("--proxy-server=http://" + prox);
		
		//options.setCapability("proxy",proxy);
		 driver= new ChromeDriver(options);
		Log.info("Chrome Browser is launched with proxy");
		driver.manage().window().maximize();
		return driver;
	}


	


	

	public static void CloseBrowser(WebDriver driver) {
		System.out.println("closing .. :" + driver.getTitle());
		driver.close();
	}



	public static WebDriver getIE8NoProxy() {
		WebDriver driver = null;
		//
//      DesiredCapabilities dc = new DesiredCapabilities();
//
////        dc.setCapability(InternetExplorerDriver.INTRODUCE_FLAKINESS_BY_IGNORING_SECURITY_DOMAINS , true);
////        dc.setCapability(InternetExplorerDriver.REQUIRE_WINDOW_FOCUS , true);
////
////        Proxy p = new Proxy();
////
////        p.setProxyType(ProxyType.DIRECT);
////        dc.setCapability(CapabilityType.PROXY, p);
////        dc.setCapability(InternetExplorerDriver.IE_ENSURE_CLEAN_SESSION, true);
////        dc.setCapability(CapabilityType.ACCEPT_SSL_CERTS, true);
////        dc.setBrowserName("internet explorer");
////        dc.setVersion("IE11");
////        dc.setCapability("ignoreProtectedModeSettings", true);
//      dc.setCapability(CapabilityType.UNEXPECTED_ALERT_BEHAVIOUR, UnexpectedAlertBehaviour.IGNORE);
//      // initiate driver
//      System.setProperty("webdriver.ie.driver",GenericUtils.getProperty("IE8Driver"));
//
//      @SuppressWarnings("unused")
//      //InternetExplorerOptions options = new InternetExplorerOptions();
////        options.setCapability(InternetExplorerDriver.REQUIRE_WINDOW_FOCUS, true);
//      //@SuppressWarnings("deprecation")
//      //WebDriver driver = new InternetExplorerDriver(dc);
//
		return driver;
	}
	
	
	public static WebDriver getBrowserStackBrowserOrig(){
		WebDriver driver=null;
		
		URL = "https://" + CommonUtils.getVal("USERNAME") + ":" + CommonUtils.getVal("AUTOMATE_KEY")
				+ "@hub-cloud.browserstack.com/wd/hub";
		
		DesiredCapabilities caps = new DesiredCapabilities();
		ChromeOptions options = new ChromeOptions();
		options.setExperimentalOption("excludeSwitches",Arrays.asList("disable-popup-blocking"));
		caps.setCapability(ChromeOptions.CAPABILITY, options);
			
		caps.setCapability("browser", CommonUtils.getVal("browser"));
		caps.setCapability("browser_version", CommonUtils.getVal("browser_version"));
		caps.setCapability("os", CommonUtils.getVal("os"));
		caps.setCapability("os_version", CommonUtils.getVal("os_version"));
				
		caps.setCapability("project",  CommonUtils.getVal("project")); // test name
		caps.setCapability("name", CommonUtils.getFeatureFileName(GlobalVariable.scenario)); // test name
	    caps.setCapability("build", CommonUtils.getVal("buildName")); // CI/CD job or build name

		caps.setCapability("acceptSslCert", "true");
		caps.setCapability("browserstack.networkLogs",  CommonUtils.getVal("browserstack.networkLogs"));
		caps.setCapability("browserstack.tunnel",  CommonUtils.getVal("browserstack.tunnel"));
		caps.setCapability("browserstack.local",  CommonUtils.getVal("browserstack.local"));
		caps.setCapability("browserstack.debug",  CommonUtils.getVal("browserstack.debug"));

		System.getProperties().put("http.proxyHost", CommonUtils.getVal("proxyHost"));
		System.getProperties().put("http.proxyPort", CommonUtils.getVal("proxyPort"));
		System.getProperties().put("http.proxyUser", CommonUtils.getVal("proxyUser"));
		System.getProperties().put("http.proxyPassword", CommonUtils.getVal("proxyPassword"));

		// For HTTPS
		System.getProperties().put("https.proxyHost", CommonUtils.getVal("proxyHost"));
		System.getProperties().put("https.proxyPort", CommonUtils.getVal("proxyPort"));
		System.getProperties().put("https.proxyUser", CommonUtils.getVal("proxyUser"));
		System.getProperties().put("https.proxyPassword", CommonUtils.getVal("proxyPassword"));
		
		
		try {
			driver = new RemoteWebDriver(new URL(URL), caps);
			driver.manage().window().maximize();
			driver.manage().deleteAllCookies();
			//driver.navigate().refresh();

		} catch (MalformedURLException e) {
		
			Log.error("Check URL once. " + e);
			e.printStackTrace();
		}
		return driver;
	}
	
	
	public static WebDriver getBrowserStackBrowser(){
		WebDriver driver=null;
		
		URL = "https://" + CommonUtils.getVal("USERNAME") + ":" + CommonUtils.getVal("AUTOMATE_KEY")
				+ "@hub-cloud.browserstack.com/wd/hub";
		
		DesiredCapabilities caps = new DesiredCapabilities();
		ChromeOptions options = new ChromeOptions();
		options.setExperimentalOption("excludeSwitches",Arrays.asList("disable-popup-blocking"));
		caps.setCapability(ChromeOptions.CAPABILITY, options);
			
		caps.setCapability("browser", CommonUtils.getVal("browser"));
		caps.setCapability("browser_version", CommonUtils.getVal("browser_version"));
		caps.setCapability("os", CommonUtils.getVal("os"));
		caps.setCapability("os_version", CommonUtils.getVal("os_version"));
				
		caps.setCapability("project",  CommonUtils.getVal("project")); // test name
		caps.setCapability("name", CommonUtils.getFeatureFileName(GlobalVariable.scenario)); // test name
	    caps.setCapability("build", CommonUtils.getVal("buildName")); // CI/CD job or build name

		caps.setCapability("acceptSslCert", "true");
		caps.setCapability("browserstack.networkLogs",  CommonUtils.getVal("browserstack.networkLogs"));
		caps.setCapability("browserstack.tunnel",  CommonUtils.getVal("browserstack.tunnel"));
		caps.setCapability("browserstack.local",  CommonUtils.getVal("browserstack.local"));
		caps.setCapability("browserstack.debug",  CommonUtils.getVal("browserstack.debug"));
		
		
		
		String mode = System.getProperty("app.mode");

		Log.info("mode selected ---> " + mode);

		if (CommonUtils.checknullString(mode)){
			Log.info("No parameters from Maven command. User Proxy selected as "+CommonUtils.getVal("proxyHostlocal"));
			System.getProperties().put("http.proxyHost", CommonUtils.getVal("proxyHostlocal"));
			System.getProperties().put("https.proxyHost", CommonUtils.getVal("proxyHostlocal"));
		}
		else{
			Log.info("Parameters from Maven command. Mode :"+mode+" and User Proxy selected as "+CommonUtils.getVal("proxyHost"));
			System.getProperties().put("http.proxyHost", CommonUtils.getVal("proxyHost"));
			System.getProperties().put("https.proxyHost", CommonUtils.getVal("proxyHost"));
		}
		
		System.getProperties().put("http.proxyPort", CommonUtils.getVal("proxyPort"));
		System.getProperties().put("http.proxyUser", CommonUtils.getVal("proxyUser"));
		System.getProperties().put("http.proxyPassword", CommonUtils.getVal("proxyPassword"));

		// For HTTPS
		
		System.getProperties().put("https.proxyPort", CommonUtils.getVal("proxyPort"));
		System.getProperties().put("https.proxyUser", CommonUtils.getVal("proxyUser"));
		System.getProperties().put("https.proxyPassword", CommonUtils.getVal("proxyPassword"));
		
		
		try {
			driver = new RemoteWebDriver(new URL(URL), caps);
			driver.manage().window().maximize();
			driver.manage().deleteAllCookies();
			//driver.navigate().refresh();

		} catch (MalformedURLException e) {
		
			Log.error("Check URL once. " + e);
			e.printStackTrace();
		}
		return driver;
	}
	
	
	
	

}