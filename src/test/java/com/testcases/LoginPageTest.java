package com.testcases;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.anuragdeb.helper.BaseFactory;
import com.pages.LoginPage;

public class LoginPageTest extends BaseFactory {
	
	
	final static Logger log = LogManager.getLogger(LoginPageTest.class);

	LoginPageTest() {
		super();
	}

	
	@BeforeMethod
	public void beforeTest() {
		browserFactory();

	}

	@Test
	public void login()  {

		LoginPage loginPg = new LoginPage();
		loginPg.NavigateSite();
		
		Assert.assertTrue(loginPg.buttonPresent(), "Login button is not present");
		log.info("Login button is present");
		

	}

	@AfterMethod
	public void afterTest() {

		tearDown();
	}
}
