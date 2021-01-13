# HybridAutomationFramework
This repo consist of a hybrid Automation framework for Web UI Apps along with utilities supporting 3 different types of Db like
1) MemSQL uses Mariadb
2) GreenPlum uses PostGreSQL
3) Oracle uses Ojdbc

Through this framework it would be easy to access any file over SFTP.

This also includes WebDriverManager through which browsers's binary has been handled.
It has a set of utilities for web and DBs

#Web Utilities:
		public static void LoadConfig(String FileName) //Load property file once and use it all over your test case
		public static String getVal(String key) // get the value from the property file loaded
	 	public static boolean IsElementPresent(WebDriver driver, String locator, String locatorValue) //Check for any element present
	 	public static boolean Ispresent(WebDriver driver, WebElement ele) 
	 	public static String GetAnyZoneCurrentDate(String Timezone, String DateFormat) 
	 	public static boolean isNumeric(String strNum) 
	 	public static void copyFile(String source, String destination) 
	 	public static void DeleteFile(String source) 
	 	public static String getCurrentDate() 
	 	public static String getCustomisedDate(String DateFormat) 
	 	public static String getCustomisedExistDate(String Date, String CurrDateFormat, String ReqDateFormat) 
	 	public static List<String> getListRunCmdOnLinux(String command, String host, String user, String password) 
	 	public static BigDecimal truncateDecimal(double value, int numberofDecimals) 
	 	public static void Sleep(long TimeInMiliSeconds) 
	 	public static void run_SparkJobOnLinux(String command, String host, String user, String password) 
	 	public static void getExcel_OnLinux(String host, String user, String password, String readFilePath,String localPath) 
	 	public static void WaitForPageLoad(WebDriver driver, int timeOut) 
	 	public static void waitForGivenTimeInSeconds(int wTmeInSeconds) 
	 	public static void waitUntilElementIsVisible(WebDriver driver, long ITO, WebElement WebElement) 
	 	public static void waitUntilelementToBeClickable(WebDriver driver, long ITO, WebElement WebElement) 
	 	public static void waitUntilelementToBeSelected(WebDriver driver, long ITO, WebElement WebElement) 
	 	public static void waitUntiltitleContains(WebDriver driver, long ITO, String WebElement) 
	 	public static void waitUntiltitleIs(WebDriver driver, long ITO, String eTitle) 
	 	public static void ScrollTo(WebDriver driver, WebElement element) 
	 	public static void scrolldown(WebDriver driver, int ScrollBy) 
	 	public static void scrollUp(WebDriver driver) 
	 	public static String getTextValue(WebElement element) 
	 	public static void putFile(String hostname, String username, String password, String copyFrom, String copyTo) 
	 	public static void getFile(String hostname, String username, String password, String copyFrom, String copyTo)
	 	public static File createShellFile(String fileName) 
	 	public List<String> getlistfromdriver(List<WebElement> list) 
	 	public static List<String> WebEleToStringList(List<WebElement> WebElelist) 
	 	public static void zoomInZoomOut(WebDriver driver, int value) 
	 	public static void deleteFile(String path) 
	 	public static boolean matchList(List<?> list1, List<?> list2) 
	 	public static boolean isNullOrEmptyMap(Map<?, ?> map) 
	 	public static boolean checkDiffMap(Map<String, ?> map1, Map<String, ?> map2) throws Exception 
	 	public static void listALLFolders(String path) 
	 	public static List<String> listAllEndsWith(String EndsWithPattern, String FolderPath) 
	 	public static boolean CompareMultiDimList(List<?> list1, List<?> list2) 
	 	public static boolean isNullOrEmptyString(String str) 
	 	public static void takeScreenShot() 

	 	
	 	
	 	
#DB Utilities
	public static Connection GPconnect(String Host, String DBName, String User, String Pass) 
 	public static Connection MemSQLconnect(String Host, String DBName, String User, String Pass) 
 	public static Connection OracleConnect(String Host, String ServiceName, String User, String Pass) 
 	public static ResultSet executeQuery(Connection conn,String queryString) 
 	public static int countQuery(Connection conn,String queryString) 
 	public int executeNonQuery(String queryString) 
 	public static String getDBColumnDataInString(Connection conn, String query) 
 	public static String getDBColDataInString(Connection conn, String query) 
 	public static List<String> getDBRowDataInList(Connection conn, String query, String ColumnNames)
 	public List<?> resultSetToArrayList(ResultSet resultSet) 
 	public static List<List> getListDB(Connection conn, String query, int ColCount)
 	public static List<String> getDBListOneColumn(Connection conn, String query) 
 	public static void teardownConnection(Connection conn) 
 	public boolean compareResultSets(ResultSet resultSet1, ResultSet resultSet2) 