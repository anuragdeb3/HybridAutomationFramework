# HybridAutomationFramework
This repo consist of a hybrid Automation framework for Web UI Apps along with utilities supporting 3 different types of Db like
1) MemSQL uses Mariadb
2) GreenPlum uses PostGreSQL
3) Oracle uses Ojdbc

Through this framework it would be easy to access any file over SFTP.

This also includes WebDriverManager through which browsers's binary has been handled.
It has a set of utilities for web and DBs

# Web Utilities:
1.	public static void LoadConfig(String FileName) //Load property file once and use it all over your test case
2.	public static String getVal(String key) // get the value from the property file loaded
3.	public static boolean IsElementPresent(WebDriver driver, String locator, String locatorValue) //Check for any element present
4.	public static boolean Ispresent(WebDriver driver, WebElement ele) 
5.	public static String GetAnyZoneCurrentDate(String Timezone, String DateFormat) 
6.	public static boolean isNumeric(String strNum) 
7.	public static void copyFile(String source, String destination) 
8.	public static void DeleteFile(String source) 
9.	public static String getCurrentDate() 
10.	public static String getCustomisedDate(String DateFormat) 
11.	public static String getCustomisedExistDate(String Date, String CurrDateFormat, String ReqDateFormat) 
12.	public static List<String> getListRunCmdOnLinux(String command, String host, String user, String password) 
13.	public static BigDecimal truncateDecimal(double value, int numberofDecimals) 
14.	public static void Sleep(long TimeInMiliSeconds) 
15.	public static void run_SparkJobOnLinux(String command, String host, String user, String password) 
16.	public static void getExcel_OnLinux(String host, String user, String password, String readFilePath,String localPath) 
17.	public static void WaitForPageLoad(WebDriver driver, int timeOut) 
18.	public static void waitForGivenTimeInSeconds(int wTmeInSeconds) 
19.	public static void waitUntilElementIsVisible(WebDriver driver, long ITO, WebElement WebElement) 
20.	public static void waitUntilelementToBeClickable(WebDriver driver, long ITO, WebElement WebElement) 
21.	public static void waitUntilelementToBeSelected(WebDriver driver, long ITO, WebElement WebElement) 
22.	public static void waitUntiltitleContains(WebDriver driver, long ITO, String WebElement) 
23.	public static void waitUntiltitleIs(WebDriver driver, long ITO, String eTitle) 
24.	public static void ScrollTo(WebDriver driver, WebElement element) 
25.	public static void scrolldown(WebDriver driver, int ScrollBy) 
26.	public static void scrollUp(WebDriver driver) 
27.	public static String getTextValue(WebElement element) 
28.	public static void putFile(String hostname, String username, String password, String copyFrom, String copyTo) 
29.	public static void getFile(String hostname, String username, String password, String copyFrom, String copyTo)
30.	public static File createShellFile(String fileName) 
31.	public List<String> getlistfromdriver(List<WebElement> list) 
32.	public static List<String> WebEleToStringList(List<WebElement> WebElelist) 
33.	public static void zoomInZoomOut(WebDriver driver, int value) 
34.	public static void deleteFile(String path) 
35.	public static boolean matchList(List<?> list1, List<?> list2) 
36.	public static boolean isNullOrEmptyMap(Map<?, ?> map) 
37.	public static boolean checkDiffMap(Map<String, ?> map1, Map<String, ?> map2) throws Exception 
38.	public static void listALLFolders(String path) 
39.	public static List<String> listAllEndsWith(String EndsWithPattern, String FolderPath) 
40.	public static boolean CompareMultiDimList(List<?> list1, List<?> list2) 
41.	public static boolean isNullOrEmptyString(String str) 
42.	public static void takeScreenShot()

	 	
	 	
	 	
# DB Utilities
1.	public static Connection GPconnect(String Host, String DBName, String User, String Pass) 
2.	public static Connection MemSQLconnect(String Host, String DBName, String User, String Pass) 
3.	public static Connection OracleConnect(String Host, String ServiceName, String User, String Pass) 
4.	public static ResultSet executeQuery(Connection conn,String queryString) 
5.	public static int countQuery(Connection conn,String queryString) 
6.	public int executeNonQuery(String queryString) 
7.	public static String getDBColumnDataInString(Connection conn, String query) 
8.	public static String getDBColDataInString(Connection conn, String query) 
9.	public static List<String> getDBRowDataInList(Connection conn, String query, String ColumnNames)
10.	public List<?> resultSetToArrayList(ResultSet resultSet) 
11.	public static List<List> getListDB(Connection conn, String query, int ColCount)
12.	public static List<String> getDBListOneColumn(Connection conn, String query) 
13.	public static void teardownConnection(Connection conn) 
14.	public boolean compareResultSets(ResultSet resultSet1, ResultSet resultSet2)
