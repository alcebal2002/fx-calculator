package utils;

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

public class DatabaseUtils {
	
	//Logger
	private static Logger logger = LoggerFactory.getLogger(DatabaseUtils.class);
	

	public static Map<String,List<FxRate>> getHistoricalRates (final String currentCurrency, final String startDate, final String endDate) {
 
		Statement stmt = null;
		String sql = null;
		ResultSet rs = null;

		Map<String,List<FxRate>> resultMap = new HashMap<String,List<FxRate>>();
		
		try {
			logger.info ("Retrieving historical rates from database for " + currentCurrency);
			stmt = DatabaseConnection.getInstance().getConnection().createStatement();
			sql = "SELECT * FROM historico_" + currentCurrency + " WHERE fecha >= STR_TO_DATE('" + startDate + "','%Y-%m-%d') AND fecha <= STR_TO_DATE('" + endDate + "','%Y-%m-%d') ORDER BY fecha ASC, hora ASC";

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
			rs.close();
		} catch(Exception e) {
			//Handle errors for Class.forName
			logger.error ("Exception: " + e.getClass() + " - " + e.getMessage());
		} finally {
			//finally block used to close resources
			try {
				if (rs != null) {
					rs.close();
				}
			} catch (SQLException e) {
				logger.error ("SQLException: " + e.getClass() + " - " + e.getMessage());
			}

			try {
				if (stmt != null) {
					stmt.close();
				}
			} catch (SQLException e) {
				logger.error ("Exception: " + e.getClass() + " - " + e.getMessage());
			}
		}
		return resultMap;
	}
	
	public static boolean checkCurrencyTableExists (final String currentCurrency) {
		 
		Statement stmt = null;
		String sql = null;
		ResultSet rs = null;

		boolean exists = false;
		
		try {
			logger.info ("Checking if currency table exists for " + currentCurrency);
			
			stmt = DatabaseConnection.getInstance().getConnection().createStatement();
			
			sql = "SELECT UPPER(SUBSTRING(table_name, 11)) as 'currency' FROM information_schema.TABLES WHERE table_name like 'historico_" + currentCurrency + "' AND data_length > 0";
			logger.info("Executing query: " + sql);
			
			try {
			
				rs = stmt.executeQuery(sql);

				while(rs.next()) {
					//Retrieve currency name
					if ((currentCurrency.toUpperCase()).equals(rs.getString("currency").toUpperCase())) {
						exists = true;
					}
				}
				rs.close();
			} catch(Exception e) {
				//Handle errors for Class.forName
				logger.error ("Exception while executing " + sql);
				logger.debug ("Exception: " + e.getClass() + " - " + e.getMessage());
			}
	
		} catch(Exception e) {
			//Handle errors for Class.forName
			logger.error ("Exception while checking if currency table exists for " + currentCurrency);
			logger.debug ("Exception: " + e.getClass() + " - " + e.getMessage());
		} finally {
			//finally block used to close resources
			try {
				if (rs != null) {
					rs.close();
				}
			} catch (SQLException e) {
				logger.error ("SQLException: " + e.getClass() + " - " + e.getMessage());
			}

			try {
				if (stmt != null) {
					stmt.close();
				}
			} catch (SQLException e) {
				logger.error ("Exception: " + e.getClass() + " - " + e.getMessage());
			}
		}
		return exists;
	}
}