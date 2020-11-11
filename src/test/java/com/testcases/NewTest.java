package com.testcases;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import helper.Library;
import helper.PageBase;

public class NewTest extends PageBase {
  


  @BeforeTest
  public void beforeTest() {
	  browserFactory();
	  
  }
  
  @Test
  public void login() {
	  
	  driver.get(Library.getVal("URL"));
 
  }

  @AfterTest
  public void afterTest() {
	  tearDown();
  }

}
