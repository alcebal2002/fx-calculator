package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datamodel.FxRate;

public class Database {
	
	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	static String DB_URL = "jdbc:mysql://<host>:<port>/<name>";
	
	static Connection conn = null;
	 
	//Logger
	private static Logger logger = LoggerFactory.getLogger(Database.class);
	
	public static Connection getConnection (final String databaseHost, final String databasePort, final String databaseName,
			                                final String databaseUser, final String databasePass) {
		if (conn == null) {
			try {
				Class.forName("com.mysql.jdbc.Driver");
				logger.info ("Connecting to database..." + DB_URL.replaceAll("<host>", databaseHost).replaceAll("<port>", databasePort).replaceAll("<name>", databaseName));
				conn = DriverManager.getConnection(DB_URL.replaceAll("<host>", databaseHost).replaceAll("<port>", databasePort).replaceAll("<name>", databaseName),databaseUser,databasePass);
			} catch (Exception ex) {
				logger.error ("Exception: " + ex.getClass() + " - " + ex.getMessage());
			}
		}
		return conn;
	}

	public static Map<String,List<FxRate>> getHistoricalRates (final List<String> currencyPairs, final String startDate, final String endDate, 
			                                       final String databaseHost, final String databasePort, final String databaseName, 
			                                       final String databaseUser, final String databasePass) {
 
		Statement stmt = null;
		String sql = null;
		ResultSet rs = null;
		List<String> existingCurrencies = new ArrayList<String>();
		Map<String,List<FxRate>> resultMap = new HashMap<String,List<FxRate>>();
		
		try {
			
			stmt = getConnection(databaseHost,databasePort,databaseName,databaseUser,databasePass).createStatement();
			
			sql = "SELECT UPPER(SUBSTRING(table_name, 11)) as 'currency' FROM information_schema.TABLES WHERE table_name like 'historico_%' and data_length > 0";
			logger.info("Executing query: " + sql);
			
			try {
			
				rs = stmt.executeQuery(sql);

				while(rs.next()) {
					//Retrieve currency name
					existingCurrencies.add(rs.getString("currency"));
				}
			} catch(Exception e) {
				//Handle errors for Class.forName
				logger.error ("Exception while executing " + sql);
				logger.error ("Exception: " + e.getClass() + " - " + e.getMessage());
			}
			
			logger.info ("Retrieving historical rates from database");
			
			for (String currentCurrency : currencyPairs) {
				
				if (existingCurrencies.indexOf(currentCurrency.toUpperCase()) != -1) {
				
					sql = "SELECT * FROM historico_" + currentCurrency + " WHERE fecha >= STR_TO_DATE('" + startDate + "','%Y-%m-%d') AND fecha <= STR_TO_DATE('" + endDate + "','%Y-%m-%d') ORDER BY fecha ASC, hora ASC";
					logger.info("Executing query: " + sql);
					
					try {
					
						rs = stmt.executeQuery(sql);
		
						int positionId = 0;
						
						while(rs.next()) {
							//Retrieve by column name
							String conversionDate = rs.getString("fecha");
							String conversionTime = rs.getString("hora");
							float open = rs.getFloat("apertura");
							float high = rs.getFloat("alto");
							float low = rs.getFloat("bajo");
							float close = rs.getFloat("cerrar");
							
							if (!resultMap.containsKey(currentCurrency)) {
								resultMap.put(currentCurrency, new ArrayList<FxRate>());
							}
							(resultMap.get(currentCurrency)).add(new FxRate(positionId, currentCurrency, conversionDate, conversionTime, open, high, low, close));
							positionId++;
						}
						logger.debug("resultMap[" + currentCurrency + "]: " + resultMap.get(currentCurrency).size());
	
					} catch(Exception e) {
						//Handle errors for Class.forName
						logger.error ("Exception while executing " + sql);
						logger.error ("Exception: " + e.getClass() + " - " + e.getMessage());
					}
				} else {
					logger.info(currentCurrency + " -> Table not present in database, ignoring...");
				}
			}
	
		} catch(Exception e) {
			//Handle errors for Class.forName
			logger.error ("Exception: " + e.getClass() + " - " + e.getMessage());
		} finally {
			//finally block used to close resources
			try {
				if (stmt != null) {
					stmt.close();
				}
			} catch (SQLException e) {
			}// nothing we can do
			
			try {
				if(conn != null) {
					conn.close();
				}
			} catch(SQLException e) {
				logger.error ("Exception: " + e.getClass() + " - " + e.getMessage());
			}
		}
		return resultMap;
	}
}