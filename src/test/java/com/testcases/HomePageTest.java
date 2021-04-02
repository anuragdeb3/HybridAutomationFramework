package com.testcases;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.anuragdeb.helper.BaseFactory;
import com.pages.HomePage;
import com.pages.LoginPage;




/*
 * 
 * Test Author : 
 * Description of the Test Case
 * 
 * 
 * 
 * 
 */


public class HomePageTest extends BaseFactory {

	HomePageTest() {
		super();
	}



	@BeforeMethod
	public void beforeTest() {
		browserFactory();

	}

	@Test
	public void login() {

		
		LoginPage loginPg = new LoginPage();
		loginPg.NavigateSite();
		HomePage homePg = loginPg.Login();
		
		Assert.assertTrue("OrangeHRM".equals(homePg.getTitleHomePage()),"HomePage Title is not matching");
		System.out.println("Login Successful");
	}

	@AfterMethod
	public void afterTest() {

		tearDown();
	}
}
