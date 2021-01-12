package com.testcases;

import java.util.ArrayList;
import com.anuragdeb.reports.ExtentReporterNG2;
import java.util.List;

import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.anuragdeb.helper.BaseFactory;
import com.anuragdeb.helper.Library;
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
