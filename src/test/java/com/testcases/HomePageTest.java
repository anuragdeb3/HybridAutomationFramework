package com.testcases;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.anuragdeb.helper.BaseFactory;
import com.pages.HomePage;
import com.pages.LoginPage;

public class HomePageTest extends BaseFactory {

	HomePageTest() {
		super();
	}

	@BeforeSuite
	public void s() {
		

	}

	@BeforeTest
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

	@AfterTest
	public void afterTest() {

		tearDown();
	}
}