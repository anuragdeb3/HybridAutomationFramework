package com.testcases;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.anuragdeb.helper.BaseFactory;
import com.anuragdeb.helper.Library;
import com.pages.LoginPage;

public class LoginPageTest extends BaseFactory {

	LoginPageTest() {
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
		Assert.assertTrue(loginPg.buttonPresent(), "Login button is not present");
		System.out.println("Login button is present");
		

	}

	@AfterTest
	public void afterTest() {

		tearDown();
	}
}
