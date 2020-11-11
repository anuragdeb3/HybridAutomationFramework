
package dbUtils;

import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/*import org.apache.log4j.Logger;
import org.junit.Assert;*/
import org.postgresql.util.PSQLException;

import helper.Library;





public class DBUtils {

	private static Connection conn = null;
	//final static Logger log = Logger.getLogger(DBFunctionsDD.class);
	
	/**
	 * @author Anurag_Deb
	 * This method would connect to GreenPlum Database
	 * @param Host
	 * @param DBName
	 * @param User
	 * @param Pass
	 * @return Greenplum Connection/Session
	 * @throws Exception
	 */

	public static Connection GPconnect(String Host, String DBName, String User, String Pass) throws Exception {

		String URL = "jdbc:postgresql://" + Host + ":" + Library.getVal("CLPort") + "/" + DBName+"?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory&sslmode=require";
		URL = URL.trim().toString();
		try {
			//jdbc:postgresql://ddlgpmtst11.us.dell.com:6432/gp_ns_ddl_test?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory&prepareThreshold=0
			///*HTMLReport.updateDetailsHTMLReport("Connection String", "Connection String Set as " + URL);
			conn = DriverManager.getConnection(URL, User, Pass);

			if (!conn.isClosed()) {
				System.out.println("Connected to GP Host " + URL);
			//	/*HTMLReport.updateDetailsHTMLReport("Connection Status", "Connected to GP Host" + URL);
			}

		} catch (NullPointerException e) {
			System.out.println("Kindly Check if the parameters are blank or not");
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"Null pointer Exception Raised",
					"Kindly Check if the parameters are blank or not, Nullpointer Excetion at GPconnect Method");
			System.out.println("NullPointer Exception raised at GPconnect methods: " +e);
			//Assert.fail();*/
		} catch (PSQLException e) {
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"PSQLExceptionn Raised", "PSQL Exception raised at GPconnect methods:");
			
			//Assert.fail();*/
			System.out.println("PSQL Exception raised at GPconnect methods: " +e);
			
		}  catch (SQLTimeoutException e) {
			System.out.println("SQL CMD Timeout Exception at GPConnect method " +e);
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"SQLTimeoutException Raised", "SQL Timeout Exception raised at GPconnect methods");*/
			//Assert.fail();
			e.printStackTrace();
			
		} catch (SQLException e) {
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"SQLException Raised", "Cause" + e.getCause() + "Message " + e.getMessage());*/

			System.out.println("SQLException has been raised at GPConnect Method." +e);
			e.printStackTrace();
			//Assert.fail();
		}

		catch (Exception e) {
			System.out.println("Generic Exception has been raised at GPConnect Method." +e);
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"Generic Exception Raised", "Generic Exception raised at GPconnect methods");*/

			e.printStackTrace();
			//Assert.fail();
		}

		return conn;
	}
	
	
	/**
	 * @author Anurag_Deb
	 * This method would connect to GreenPlum Database
	 * @param Host
	 * @param DBName
	 * @param User
	 * @param Pass
	 * @return MemSQL Connection/Session
	 * @throws Exception
	 */

	public static Connection MemSQLconnect(String Host, String DBName, String User, String Pass) throws Exception {

		// Added &useSSL=true&verifyServerCertificate=false"; because access was denied due to AuthenticationException
		// This will bypass the certficates
		
		//String URL = "jdbc:mysql://" + Host + ":"+Library.getVal("MemSQLPort") +"/"+ DBName + "?useLegacyDatetimeCode=false&serverTimezone=UTC&useSSL=true&verifyServerCertificate=false";
		
		String URL ="jdbc:mariadb://" + Host + ":3306/"+ DBName + "?useLegacyDatetimeCode=false&serverTimezone=UTC&useSSL=true&trustServerCertificate=true";
		
	//	String URL ="jdbc:mariadb://" + Host + ":"+Library.getVal("MemSQLPort") +"/"+ DBName + "?useLegacyDatetimeCode=false&serverTimezone=UTC&useSSL=true&trustServerCertificate=true";
		
		
		URL = URL.toString().trim();
		Connection conn = null;

		try {

			/*HTMLReport.updateDetailsHTMLReport("Connection String", "Connection String Set as " + URL);*/

			conn = DriverManager.getConnection(URL, User, Pass);
			if (!conn.isClosed()) {
				System.out.println("Connected to Memsql Host " + URL);
				/*HTMLReport.updateDetailsHTMLReport("Connection Status", "Connected to MemSQL Host" + Host);*/
			}

		}

		catch (NullPointerException e) {
			System.out.println("Kindly Check if the parameters are blank or not");
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"Null pointer Exception Raised at MemSQLconnect method",
					"Kindly Check if the parameters are blank or not" + e.getCause() + "Message " + e.getMessage());*/
			System.out.println("NullPointer Exception : "+e);
			//Assert.fail();
		}  catch (SQLTimeoutException e) {
			System.out.println("SQL CMD Timeout Exception: " +e);
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"SQLTimeoutException Raised", "SQL Timeout Exception raised at MemSQLconnect method");*/

			e.printStackTrace();
			//Assert.fail();
		} catch (SQLException e) {
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"SQLException Raised", "SQl Exception raised at MemSQLconnect method");*/

			System.out.println("SQLException has been raised at MemSQLconnect method." +e);
			e.printStackTrace();
			//Assert.fail();
		}

		catch (Exception e) {
			System.out.println("Generic Exception has been raised." +e);
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"Generic Exception Raised", "Check MemSQLconnect method");*/

			e.printStackTrace();
			//Assert.fail();
		}

		return conn;

	}
	
	/**
	 * @author Anurag_Deb
	 * This Method will connect to Oracle Database
	 * @param Host
	 * @param ServiceName
	 * @param User
	 * @param Pass
	 * @return Oracle Connection/Session
	 * @throws Exception
	 */
		

	public static Connection OracleConnect(String Host, String ServiceName, String User, String Pass) throws Exception {
		try {
			String URL = "jdbc:oracle:thin:@" + Host.trim() + ":1521/" + ServiceName.trim();
			System.out.println(URL);

			if (conn != null && !conn.isClosed()) {

				System.out.println("Closing the previous existing connection");
				conn.close();
			}
			/*HTMLReport.updateDetailsHTMLReport("Connection String", "Connection String Set as " + URL);*/
			conn = DriverManager.getConnection(URL, User, Pass);
			
			System.out.println("----Connected");
			if (!conn.isClosed()) {
				System.out.println("Connected to Oracle Host" + Host);
				/*HTMLReport.updateDetailsHTMLReport("Connection Status", "Connected to Oracle Host" + Host);*/
			}

		} catch (NullPointerException e) {
			System.out.println("Kindly Check if the parameters are blank or not");
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"Null pointer Exception Raised",
					"Kindly Check if the parameters are blank or not. check OracleConnect method ");*/
			System.out.println("NullPointer Exception : "+e);
			//Assert.fail();
		}   catch (SQLTimeoutException e) {
			System.out.println("SQL CMD Timeout Exception: " + e.getCause());
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"SQLTimeoutException Raised", "Cause" + e.getCause() + "Message " + e.getMessage());*/


			//Assert.fail();
		} catch (SQLException e) {
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"SQLException Raised", "Cause" + e.getCause() + "Message " + e.getMessage());*/

			System.out.println("SQLException has been raised. Check Jars Compatibility :" +e);

			//Assert.fail();
		}

		catch (Exception e) {
			System.out.println("Generic Exception has been raised." +e);
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"Generic Exception Raised", "Cause" + e.getCause() + "Message " + e.getMessage());*/
			

			//Assert.fail();
		}

		return conn;
	}

	/**
	 * @author Anurag_Deb
	 * This method helps to fetch rows/columns from Database
	 * @param queryString
	 * @return resultSet if it is not Empty
	 * @throws Exception
	 */
	public static ResultSet executeQuery(Connection conn,String queryString) throws Exception {

		ResultSet resultSet = null;

		try {
			Statement stm = conn.createStatement();
			resultSet = stm.executeQuery(queryString);
			if (!resultSet.isBeforeFirst()) {
				System.out.println("ResultSet in empty");
				/*HTMLReport.updateDetailsHTMLReport("Result Set Status","Result is empty");*/
			}

			/*else
				return resultSet;*/
		} catch (NullPointerException e) {
			System.out.println("Kindly Check if the parameters are blank or not");
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"Null pointer Exception Raised",
					"Kindly Check if the parameters are blank or not" + e.getCause() + "Message " + e.getMessage());*/
			System.out.println("NullPointer Exception : " +e);
			//Assert.fail();
		}   catch (SQLTimeoutException e) {
			System.out.println("SQL CMD Timeout Exception: "+e);
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"SQLTimeoutException Raised", "Cause" + e.getCause() + "Message " + e.getMessage());*/

			e.printStackTrace();
			//Assert.fail();
		} catch (SQLException e) {
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"SQLException Raised", "Cause" + e.getCause() + "Message " + e.getMessage());*/

			System.out.println("SQLException has been raised." +e);
			e.printStackTrace();
			//Assert.fail();
		}

		catch (Exception e) {
			System.out.println("Generic Exception has been raised." +e);
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"Generic Exception Raised", "Cause" + e.getCause() + "Message " + e.getMessage());*/
			
			e.printStackTrace();
			//Assert.fail();
		}

		/*finally {
			conn.close();
		}*/
		System.out.println("REMEMBER TO CLOSE THE RESULTSET");
		
		return resultSet;

	}

	/**
	 * @author Anurag_Deb
	 * Used for having value from Aggregate functions
	 * @param conn
	 * @param queryString
	 * @return
	 * @throws Exception
	 */

	public static int countQuery(Connection conn,String queryString) throws Exception {
		int value = 0;
		try {
			System.out.println("Query : "+queryString);
			Statement stm = conn.createStatement();
			ResultSet resultSet = stm.executeQuery(queryString);
			while (resultSet.next()) {
				value = Integer.parseInt(resultSet.getString(1));
				//System.out.println(value);
			}
			
			
			return value;
		} catch (NullPointerException e) {
			System.out.println("Kindly Check if the parameters are blank or not");
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"Null pointer Exception Raised",
					"Kindly Check if the parameters are blank or not" + e.getCause() + "Message " + e.getMessage());*/
			System.out.println("NullPointer Exception at countQuery method: "+e);
			//Assert.fail();
		}  catch (SQLTimeoutException e) {
			System.out.println("SQL CMD Timeout Exception at countQuery method: " +e);
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"SQLTimeoutException Raised", "Cause" + e.getCause() + "Message " + e.getMessage());*/

			e.printStackTrace();
			//Assert.fail();
		} catch (SQLException e) {
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"SQLException Raised", "Exception raise at countQuery method");*/

			System.out.println("SQLException has been raised." +e);
			e.printStackTrace();
			//Assert.fail();
		}

		catch (Exception e) {
			System.out.println("Generic Exception has been raised." +e);
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"Generic Exception Raised", "Generic Exception raise at countQuery method");*/
			
			
			//Assert.fail();
		}
		finally {
			conn.close();
		}
		return value;

	}

	/**
	 * @author Anurag_Deb
	 * @param queryString
	 *            a SQL statement
	 * @return single value result of SQL statement
	 *
	 *         This method is used for SQL statements which update the database in
	 *         some way. For example INSERT, UPDATE and DELETE statements. All these
	 *         statements are DML(Data Manipulation Language) statements. This
	 *         method can also be used for DDL(Data Definition Language) statements
	 *         which return nothing. For example CREATE and ALTER statements. This
	 *         method returns an int value which represents the number of rows
	 *         affected by the query. This value will be 0 for the statements which
	 *         return nothing.
	 * @throws Exception
	 */

	public int executeNonQuery(String queryString) throws Exception {
		int result = 0;

		try {
			// println queryString
			Statement stm = conn.createStatement();

			// Object result = stm.execute(queryString)
			result = stm.executeUpdate(queryString);

			if (result == 0) {
				System.out.println("Nothing returned");
				return result;
			}

			else if (result == 1)
				System.out.println("Executed Successfully");
			else
				System.out.println("No of Rows Returned = " + result);
			return result;
		}

		catch (NullPointerException e) {
			System.out.println("Kindly Check if the parameters are blank or not");
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"Null pointer Exception Raised",
					"Kindly Check if the parameters are blank or not" + e.getCause() + "Message " + e.getMessage());*/
			System.out.println("NullPointer Exception : " + e.getMessage() + "/" + e.getCause());
			//Assert.fail();
		}  catch (SQLTimeoutException e) {
			System.out.println("SQL CMD Timeout Exception: " + e.getCause());
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"SQLTimeoutException Raised", "Cause" + e.getCause() + "Message " + e.getMessage());*/

			e.printStackTrace();
			//Assert.fail();
		} catch (SQLException e) {
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"SQLException Raised", "Cause" + e.getCause() + "Message " + e.getMessage());*/

			System.out.println("SQLException has been raised." +e);
			e.printStackTrace();
			//Assert.fail();
		}

		catch (Exception e) {
			System.out.println("Generic Exception has been raised." +e);
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"Generic Exception Raised", "Cause" + e.getCause() + "Message " + e.getMessage());*/
			
			e.printStackTrace();
			//Assert.fail();
		}

		return result;

	}
	
	
/**
 * 	
 * @param conn
 * @param query
 * @return
 * @throws Exception
 */


	public static String getDBColumnDataInString(Connection conn, String query) throws Exception {

		String value = null;
		Statement st = null;
		ResultSet rs1 = null;
		if (conn != null) {
			System.out.println("DB Connected");
			try {
				st = conn.createStatement();
				rs1 = st.executeQuery(query);
				if (rs1.next()) {
					//System.out.println("Data of Column '" + ColumnName + "' : " + rs1.getString(ColumnName));
					value = rs1.getString(1);
				}
			} catch (SQLTimeoutException e) {
				System.out.println("SQL CMD Timeout Exception raised at getGreenPlumColumnDataInString method "+e );
				/*HTMLReport.updateErrorMessageintoHTMLReport("Exception getGreenPlumColumnDataInString method",
						"SQL CMD Timeout Exception:" + e.getCause() + "Message " + e.getMessage());*/
			
				//Assert.fail();
			} catch (SQLException e) {

				System.out.println("SQLException has been raised at getGreenPlumColumnDataInString method."+e);
				
				/*HTMLReport.updateErrorMessageintoHTMLReport("Exception raised at getGreenPlumColumnDataInString method :",
						"SQLException has been raised." + "   Cause : " + e.getCause() + "  Message:" + e.getMessage());*/

				
				//Assert.fail();
			} catch (NullPointerException e) {
				System.out.println("Kindly Check if the parameters are blank or not");
				/*HTMLReport.updateErrorMessageintoHTMLReport("Null pointer Exception Raised",
						"Kindly Check if the parameters are blank or not" + e.getCause() + "Message " + e.getMessage());*/
				System.out.println("NullPointer Exception : "+e);
				//Assert.fail();
			}

			catch (Exception e3) {

				/*HTMLReport.updateErrorMessageintoHTMLReport("Exception Raised",
						"Generic Exception:",e3);*/
				
				//Assert.fail();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					System.out.println("SQLException has been raised." +e);
					
					//Assert.fail();
				}
			}

		}
		return value;

	}

	
	
	public static String getDBColDataInString(Connection conn, String query) throws Exception {

		String value = null;
		Statement st = null;
		ResultSet rs1 = null;
		if (conn != null) {
			System.out.println("GreenPlum Connected");
			try {
				st = conn.createStatement();
				rs1 = st.executeQuery(query);
				if (rs1.next()) {
					//System.out.println("Data of Column '" + ColumnName + "' : " + rs1.getString(ColumnName));
					value = rs1.getString(1);
				}
			} catch (SQLTimeoutException e) {
				System.out.println("SQL CMD Timeout Exception raised at getGreenPlumColumnDataInString method "+e );
				/*HTMLReport.updateErrorMessageintoHTMLReport("Exception getGreenPlumColumnDataInString method",
						"SQL CMD Timeout Exception:" + e.getCause() + "Message " + e.getMessage());*/
			
				//Assert.fail();
			} catch (SQLException e) {

				System.out.println("SQLException has been raised at getGreenPlumColumnDataInString method."+e);
				
				/*HTMLReport.updateErrorMessageintoHTMLReport("Exception raised at getGreenPlumColumnDataInString method :",
						"SQLException has been raised." + "   Cause : " + e.getCause() + "  Message:" + e.getMessage());*/

				
				//Assert.fail();
			} catch (NullPointerException e) {
				System.out.println("Kindly Check if the parameters are blank or not");
				/*HTMLReport.updateErrorMessageintoHTMLReport("Null pointer Exception Raised",
						"Kindly Check if the parameters are blank or not" + e.getCause() + "Message " + e.getMessage());*/
				System.out.println("NullPointer Exception : "+e);
				//Assert.fail();
			}

			catch (Exception e3) {

				/*HTMLReport.updateErrorMessageintoHTMLReport("Exception Raised",
						"Generic Exception:",e3);*/
				
				//Assert.fail();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					System.out.println("SQLException has been raised." +e);
					
					//Assert.fail();
				}
			}

		}
		return value;

	}

	

	public static List<String> getDBRowDataInList(Connection conn, String query, String ColumnNames)
			throws Exception {

		System.out.println("Query : "+query);
		Statement st = null;
		ResultSet rs1 = null;
		String value = "";
		List<String> list = new ArrayList<String>();
		String[] colNames = ColumnNames.split(":");
		System.out.println(Arrays.toString(colNames));
		
		
		if (conn != null) {
			System.out.println("DB Connected");
			try {
				st = conn.createStatement();
				rs1 = st.executeQuery(query);
				if (rs1.next()) {
					for (int i = 0; i < colNames.length; i++) {

						value = rs1.getString(colNames[i]);
						//System.out.println(colNames[i]+"----"+value);
						try {

							if (value.contains("%")) {
								value = value.replace("%", "");
							}
						} catch (NullPointerException n) {
							// TODO Auto-generated catch block
							value = "null";
							// n.printStackTrace();
						}
						list.add(value);
						//System.out.println(value);
					}
					//System.out.println(list);
					System.out.println(list);

				}

			} catch (SQLTimeoutException e) {
				System.out.println("SQL CMD Timeout Exception at getGreenPlumRowDataWithList: " +e);
				/*HTMLReport.updateErrorMessageintoHTMLReport("Exception Raised at getGreenPlumRowDataWithList",
						"SQL CMD Timeout Exception:" + e.getCause() + "Message " + e.getMessage());*/
				
				//Assert.fail();
			} catch (SQLException e) {

				/*HTMLReport.updateErrorMessageintoHTMLReport("Exception raised :",
						"SQLException has been raised at getGreenPlumRowDataWithList." + "   Cause : " + e.getCause() + "  Message:" + e.getMessage());*/

				System.out.println("SQLException has been raised at getGreenPlumRowDataWithList." + "   Cause : " + e.getCause() + "  Message:" + e.getMessage());
				
				//Assert.fail();
			} catch (NullPointerException e) {
				System.out.println("Kindly Check if the parameters are blank or not");
				System.out.println("NullPointer Exception raised at getGreenPlumRowDataWithList : " + e );
				
				/*HTMLReport.updateErrorMessageintoHTMLReport("Null pointer Exception Raised at getGreenPlumRowDataWithList",
						"Kindly Check if the parameters are blank or not" + e.getCause() + "Message " + e.getMessage());*/
				//Assert.fail();
			}

			catch (Exception e3) {
				System.out.println("Generic Exception raised at getGreenPlumRowDataWithList : " + e3 );
				
				/*HTMLReport.updateErrorMessageintoHTMLReport("Exception Raised at getGreenPlumRowDataWithList",
						"Generic Exception:" + e3.getCause() + "Message " + e3.getMessage());*/
				
				//Assert.fail();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					System.out.println("SQLException has been raised." + "   Cause : " + e.getCause() + "  Message:" + e.getMessage());
					
					//Assert.fail();
				}
			}

		}
		return list;

	}


	
	
	
	
	
	/**
	 * @author Anurag_Deb
	 * @param resultSet
	 * @return List of ColumnName and Column Value
	 * @throws Exception
	 */

	public List<?> resultSetToArrayList(ResultSet resultSet) throws Exception {
		ResultSetMetaData md = resultSet.getMetaData();
		int columns = md.getColumnCount();
		ArrayList<Object> list = new ArrayList<Object>();

		try {
			while (resultSet.next()) {
				HashMap<String, Object> row = new HashMap<String, Object>(columns);
				for (int i = 1; i <= columns; ++i) {
					row.put(md.getColumnName(i), resultSet.getObject(i));
				}
				list.add(row);
			}
		} catch (NullPointerException e) {
			System.out.println("Kindly Check if the parameters are blank or not");
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"Null pointer Exception Raised",
					"Kindly Check if the parameters are blank or not" + e.getCause() + "Message " + e.getMessage());*/
			System.out.println("NullPointer Exception : " +e);
			//Assert.fail();
		}  catch (SQLTimeoutException e) {
			System.out.println("SQL CMD Timeout Exception: " + e.getCause());
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"SQLTimeoutException Raised", "Cause" + e.getCause() + "Message " + e.getMessage());*/

			e.printStackTrace();
			//Assert.fail();
		} catch (SQLException e) {
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"SQLException Raised", "Cause" + e.getCause() + "Message " + e.getMessage());*/

			System.out.println("SQLException has been raised." +e);
			e.printStackTrace();
			//Assert.fail();
		}

		catch (Exception e) {
			System.out.println("Generic Exception has been raised." +e);
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"Generic Exception Raised", "Cause" + e.getCause() + "Message " + e.getMessage());*/
			
			e.printStackTrace();
			//Assert.fail();
		}


		return list;
	}
	
	public static List<List> getListDB(Connection conn, String query, int ColCount)
			throws Exception {

		List<List> listParent = new ArrayList<List>();			
		int col=0;							
		
		Statement st = null;
		ResultSet rs = null;
				
		if (conn != null) {
			System.out.println("DB Connected");
			try {
				st = conn.createStatement();
				rs = st.executeQuery(query);
				while (rs.next()) {
					List<Object> childlist = new ArrayList<Object>();
					
					for(col=1;col<=ColCount;col++) {
						//System.out.println(col+"---"+rs.getString(col));
						childlist.add(rs.getString(col));
						
					}
				listParent.add(childlist);	
				
				}

			} catch (SQLTimeoutException e) {
				System.out.println("SQL CMD Timeout Exception at getListDB: " +e);
				/*HTMLReport.updateErrorMessageintoHTMLReport("Exception Raised at getListDB",
						"SQL CMD Timeout Exception:" + e.getCause() + "Message " + e.getMessage());*/
				
				//Assert.fail();
			} catch (SQLException e) {

				/*HTMLReport.updateErrorMessageintoHTMLReport("Exception raised :",
						"SQLException has been raised at getListDB." + "   Cause : " + e.getCause() + "  Message:" + e.getMessage());*/

				System.out.println("SQLException has been raised at getListDB." + e);
				
				//Assert.fail();
			} catch (NullPointerException e) {
				System.out.println("Kindly Check if the parameters are blank or not");
				System.out.println("NullPointer Exception raised at getListDB : " +e );
				
				/*HTMLReport.updateErrorMessageintoHTMLReport("Null pointer Exception Raised at getListDB",
						"Kindly Check if the parameters are blank or not" + e.getCause() + "Message " + e.getMessage());*/
				//Assert.fail();
			}

			catch (Exception e3) {
				System.out.println("Generic Exception raised at getListDB : " + e3 );
				
				/*HTMLReport.updateErrorMessageintoHTMLReport("Exception Raised at getListDB",
						"Generic Exception:" + e3.getCause() + "Message " + e3.getMessage());*/
				
				
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					System.out.println("SQLException has been raised." + "   Cause : " + e.getCause() + "  Message:" + e.getMessage());
					
					//Assert.fail();
				}
			}

		}
		return listParent;

	}

	public static List<String> getDBListOneColumn(Connection conn, String query) throws Exception {
		Statement st = null;
		ResultSet rs1 = null;
		List<String> inputs = new ArrayList<String>();
		if (conn != null) {
			System.out.println("DB Connected");
			try {
				st = conn.createStatement();
				rs1 = st.executeQuery(query);
				while (rs1.next()) {
					inputs.add(rs1.getString(1));
				}
			}  catch (SQLTimeoutException e) {
				System.out.println("SQL CMD Timeout Exception: " + e.getCause());
				/*HTMLReport.updateErrorMessageintoHTMLReport("Exception Raised",
						"SQL CMD Timeout Exception:" + e.getCause() + "Message " + e.getMessage());*/
				
				//Assert.fail();
			} catch (SQLException e) {

				/*HTMLReport.updateErrorMessageintoHTMLReport("Exception raised :",
						"SQLException has been raised." + "   Cause : " + e.getCause() + "  Message:" + e.getMessage());*/

				System.out.println("SQLException has been raised." + e);
				
				//Assert.fail();
			} catch (NullPointerException e) {
				System.out.println("Kindly Check if the parameters are blank or not" + e);
				/*HTMLReport.updateErrorMessageintoHTMLReport("Null pointer Exception Raised",
						"Kindly Check if the parameters are blank or not" + e.getCause() + "Message " + e.getMessage());*/
	
				//Assert.fail();
			}

			catch (Exception e) {
				System.out.println("Generic Exception has been raised at validateGreenPlumpSqlMRPData method :"+ e);
				/*HTMLReport.updateErrorMessageintoHTMLReport("Exception Raised",
						"Generic Exception:" + e.getCause() + "Message " + e.getMessage());*/
				
				//Assert.fail();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					
					//Assert.fail();
				}
			}

		}
		return inputs;

	}
	
	
	
	
	
	
	
	
	
	
	/**
	 * Closes all connection/Session if opened
	 * @param conn
	 * @throws Exception
	 */

	public static void teardownConnection(Connection conn) throws Exception {

		try {
			if (!conn.isClosed()) {
				conn.close();
			}
		} catch (SQLException e) {
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"SQLException Raised", "Cause" + e.getCause() + "Message " + e.getMessage());*/

			System.out.println("SQLException has been raised." + e);
			e.printStackTrace();
			//Assert.fail();
		}

		catch (Exception e) {
			System.out.println("Generic Exception has been raised." +e);
			
			/*HTMLReport.updateErrorMessageintoHTMLReport("Cause : " + e.getCause() + "Message " + e.getMessage(),
					"Generic Exception Raised", "Cause" + e.getCause() + "Message " + e.getMessage());*/
			
			e.printStackTrace();
			//Assert.fail();
			
		}
	}
	
	public boolean compareResultSets(ResultSet resultSet1, ResultSet resultSet2) throws SQLException{
		boolean flag  = true;
		while (resultSet1.next()) {
			resultSet2.next();
			ResultSetMetaData resultSetMetaData = resultSet1.getMetaData();
			int count = resultSetMetaData.getColumnCount();
			for (int i = 1; i <= count; i++) {
				if (!resultSet1.getObject(i).equals(resultSet2.getObject(i))) {

					System.out.println(resultSet1.getObject(i));
					System.out.println(resultSet2.getObject(i));
					flag = false;
				}
				else{
					System.out.println(resultSet1.getObject(i) + " =  "+resultSet2.getObject(i));
				}

			}
		}
		return flag;
	}
	
	

}
