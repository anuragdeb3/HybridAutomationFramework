package com.testcases;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.anuragdeb.helper.BaseFactory;
import com.pages.LoginPage;

public class NewTest extends BaseFactory {

	NewTest() {
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
		loginPg.Login();

	}

	@AfterTest
	public void afterTest() {

		tearDown();
	}
}
