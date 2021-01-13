package com.pages;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;

import com.anuragdeb.helper.BaseFactory;

public class HomePage extends BaseFactory{
	
	@FindBy(id="welcome")
	WebElement username;
	
	
	
	
	public String getTitleHomePage() {
		return driver.getTitle();
	}

}
