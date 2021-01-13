package com.pages;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.pagefactory.AjaxElementLocatorFactory;

import com.anuragdeb.helper.BaseFactory;
import com.anuragdeb.helper.Library;

public class LoginPage extends BaseFactory {
	
	@FindBy(id="txtUsername")
	WebElement username;
	
	@FindBy(id="txtPassword")
	WebElement password;
	
	@FindBy(id="btnLogin")
	WebElement loginBtn;
	
	public LoginPage(){
		
		//lazy initialization
		//Wait will start only if we do perform any operation on the element.
		AjaxElementLocatorFactory factory = new AjaxElementLocatorFactory(driver, 100);
		PageFactory.initElements(factory, this);
	}
	
	
	
	public void NavigateSite() {
	 driver.get(Library.getVal("URL"));
	
	}
	
	public boolean buttonPresent() {
		return Library.IsElementPresent(driver, "id","btnLogin");
	}
	
	
	public HomePage Login() {
		username.sendKeys(Library.getVal("userName"));
		password.sendKeys(Library.getVal("passWord"));
		loginBtn.click();
		
		return new HomePage();  	//Page Chaining Model
		
		}
	
	
}
