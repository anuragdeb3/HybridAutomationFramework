package com.testcases;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.anuragdeb.helper.Library;

public class TestData {
	
	
	
	@Test(dataProvider="SearchData")
	//@Parameters({"username","password"})
	public void DatatoTest(String user, String pwd, String msg) {
		
		
		System.out.println(user+pwd+msg);
		                    
		
		
	}
	
	
	@DataProvider(name="SearchData")
	public Object[][] getData(){
		
		
		Object data[][]=Library.getData("TestData.xlsx", "Sheet1");
		
		
		return data;
		
	}
	
	
	
	@Test
	public void DatatoTest1() {
		

		                    
		
		
	}
	
	@Test
	public void DatatoTest2() {
		
		
		                    
		
		
	}
	
	
	
	
	
	
	
	
	
	
	

}
