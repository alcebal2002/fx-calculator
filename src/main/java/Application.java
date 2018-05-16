// /Library/Java/JavaVirtualMachines/jdk1.8.0_102.jdk/Contents/Home/jre/lib/rt.jar to be added to the classpath

import java.io.FileReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVReader;

import datamodel.CalcResult;
import datamodel.FxRate;
import utils.ApplicationProperties;
import utils.Database;
import utils.GeneralUtils;

public class Application {

	// Logger
	private static Logger logger = LoggerFactory.getLogger(Application.class);

	// Execution times
	private static long histDataLoadStartTime;
	private static long histDataLoadStopTime;
	private static long calculationStartTime;	
	private static long calculationStopTime;	
	
	// Application properties
	private static int numberOfRecords = 0;
	private static int printAfter = 0;
	
	private static String datasource;
	private static String historicalDataPath;
	private static String historicalDataFileExtension;
	private static String historicalDataSeparator;

	private static String databaseHost;
	private static String databasePort;
	private static String databaseName;
	private static String databaseUser;
	private static String databasePass;

	
	private static List<String> currencyPairs;
	private static String startDate;
	private static String endDate;
	private static float increasePercentage;
	private static float decreasePercentage;
	private static int maxLevels;
	
	// Lists and Maps
	private static Map<String,List<FxRate>> historicalDataMap;
	private static Map<String,CalcResult> calcResultsMap = new HashMap<String,CalcResult>();
	
    public static void main (String args[]) {

		logger.info("Application started");
		logger.info("Loading application properties from " + ApplicationProperties.getPropertiesFile());

		// Load properties from file
		loadProperties ();
    	
		// Print parameters used
		printParameters ("Start");
		
		// Load historical data
		populateHistoricalFxData();
		
		// Execute calculations
		if (historicalDataMap != null && historicalDataMap.size() > 0) {
			executeCalculations ();
		}
        
		// Print results
        printResults ();
		logger.info("Application finished");
    }

    
	// Populates historical data and puts the objects into historical data list)
    // Depending on the datasource parameter, data could be retrieved from database (mysql) or files
    // FX Historical Data format: conversionDate,conversionTime,open,high,low,close
    public static void populateHistoricalFxData () {
    	
    	logger.info("Data source set to: " + datasource);

    	histDataLoadStartTime = System.currentTimeMillis();

    	int totalCounter = 0;

    	if ("database".equals(datasource)) {
    		// Populate historical data from mysql database
    		
    		historicalDataMap = Database.getHistoricalRates(currencyPairs, startDate, endDate, databaseHost, databasePort, databaseName, databaseUser, databasePass );
    		
    		if (historicalDataMap != null && historicalDataMap.size() > 0) {
    			Iterator<Entry<String, List<FxRate>>> iter = historicalDataMap.entrySet().iterator();
    			
    			while (iter.hasNext()) {
    	            Entry<String, List<FxRate>> entry = iter.next();
    	            logger.info ("   " + entry.getKey() + " -> total records loaded " + (entry.getValue()).size());
    	        }
    		}
    	} else {
    		// Populate historical data from files
    		historicalDataMap = new HashMap<String,List<FxRate>>();
    		
    		String currentCurrency = null;
    		
        	logger.info("Populating historical data from file (ext. " + historicalDataFileExtension + ") from "+ historicalDataPath);
        	logger.info("Looking for " + currencyPairs.toString() + " files");
        	
        	List<String> dataFiles = GeneralUtils.getFilesFromPath(historicalDataPath,historicalDataFileExtension);
        	
        	for(String dataFile : dataFiles){
        		
        		currentCurrency = dataFile.substring(0,dataFile.indexOf("."));
        		
        		if (currencyPairs.contains(currentCurrency)) {
                	logger.info ("Populating historical FX data from " + dataFile + "...");
                	
                	try {
                		CSVReader reader = new CSVReader(new FileReader(historicalDataPath + dataFile));
            	        String [] nextLine;
            	        while ((nextLine = reader.readNext()) != null) {
            	        	
            	        	FxRate fxRate = new FxRate (currentCurrency,nextLine,totalCounter);
            	        	
        					if (!historicalDataMap.containsKey(currentCurrency)) {
        						historicalDataMap.put(currentCurrency, new ArrayList<FxRate>());							
        					}
        					(historicalDataMap.get(currentCurrency)).add(fxRate);

        					if (totalCounter%printAfter == 0) {
            		        	logger.debug ("  " + dataFile + " -> loaded " + totalCounter + " records so far");
            				}
        					totalCounter++;
            	        }
            	        logger.info ("  " + dataFile + " -> total records loaded " + historicalDataMap.get(currentCurrency).size());
            	        reader.close();
            	    	
                	} catch (Exception ex) {
                		logger.error ("Exception in file " + dataFile + " - line " + totalCounter + " - " + ex.getClass() + " - " + ex.getMessage());
                	}
        		} else {
        			logger.info ("Data File " + dataFile + " not found in the list of in-scope currencies");
        		}
        		
        	}
    	}

    	histDataLoadStopTime = System.currentTimeMillis();
    	logger.info ("Populating historical data finished");
    }
    
    private static void executeCalculations () {
		try { 

			logger.debug ("Start calculations ");
			calculationStartTime = System.currentTimeMillis();

			float increase = 1+(increasePercentage/100);
			float decrease = 1-(decreasePercentage/100);

			CountDownLatch latch = new CountDownLatch(currencyPairs.size());
			
			for (String currentCurrency : currencyPairs) {

				Runnable process = new WorkerRunnable(historicalDataMap, currentCurrency, calcResultsMap, increase, decrease, maxLevels, latch);
				new Thread(process).start();
			}
			logger.info("Waiting for all the Workers to finish");
			latch.await();
			logger.info("All workers finished");
			
			calculationStopTime = System.currentTimeMillis();
			//logger.debug ("Finished calculations [" + totalCalculations + "]");
			
		} catch (Exception e) { 
			e.printStackTrace(); 
		} 
	} 
    
    private static void loadProperties () {

		datasource = ApplicationProperties.getStringProperty("main.datasource");
		databaseHost = ApplicationProperties.getStringProperty("database.host");
		databasePort = ApplicationProperties.getStringProperty("database.port");
		databaseName = ApplicationProperties.getStringProperty("database.db_name");  
		databaseUser = ApplicationProperties.getStringProperty("database.username");  
		databasePass = ApplicationProperties.getStringProperty("database.password");  
		
		historicalDataPath = ApplicationProperties.getStringProperty("main.historicalDataPath");
		historicalDataFileExtension = ApplicationProperties.getStringProperty("main.historicalDataFileExtension");
		historicalDataSeparator = ApplicationProperties.getStringProperty("main.historicalDataSeparator");
		
		currencyPairs = ApplicationProperties.getListProperty("execution.currencyPairs");
		startDate = ApplicationProperties.getStringProperty("execution.startDate");
		endDate = ApplicationProperties.getStringProperty("execution.endDate");
		increasePercentage = ApplicationProperties.getFloatProperty("execution.increasePercentage");
		decreasePercentage = ApplicationProperties.getFloatProperty("execution.decreasePercentage");
		maxLevels = ApplicationProperties.getIntProperty("execution.maxLevels");
		numberOfRecords = ApplicationProperties.getIntProperty("test.numberOfRecords");
		printAfter = ApplicationProperties.getIntProperty("test.printAfter");

    }
    
	// Print execution parameters 
	private static void printParameters (final String title) {
		logger.info ("");
		logger.info ("****************************************************"); 
		logger.info (title + " FXCalculator with the following parameters:"); 
		logger.info ("****************************************************"); 
		logger.info ("  - datasource               : " + datasource);
		logger.info ("  - hist. data path          : " + historicalDataPath);
		logger.info ("  - hist. data extension     : " + historicalDataFileExtension);
		logger.info ("  - hist. data separator     : " + historicalDataSeparator);

		logger.info ("  - database host            : " + databaseHost);
		logger.info ("  - database port            : " + databasePort);
		logger.info ("  - database name            : " + databaseName);
		logger.info ("  - database username        : " + databaseUser);
		logger.info ("  - database password        : " + databasePass);

		logger.info ("  - currency pairs           : " + currencyPairs.toString());
		logger.info ("  - start date               : " + startDate);
		logger.info ("  - end date                 : " + endDate);
		logger.info ("  - increase percentage      : " + increasePercentage);
		logger.info ("  - decrease percentage      : " + decreasePercentage);
		logger.info ("  - max. levels              : " + maxLevels);
		logger.info ("  - number of records [test] : " + numberOfRecords); 
		logger.info ("  - print after [test]       : " + printAfter);
		logger.info ("****************************************************");
		logger.info ("");
	}

	// Print execution times
	private static void printResults () {
		logger.info ("");
		logger.info ("Historical Data Load:");
		logger.info ("**************************************************");
		logger.info ("  - Start time  : " + new Timestamp(histDataLoadStartTime)); 
		logger.info ("  - Stop time   : " + new Timestamp(histDataLoadStopTime)); 

		long millis = histDataLoadStopTime - histDataLoadStartTime;
		long days = TimeUnit.MILLISECONDS.toDays(millis);
		millis -= TimeUnit.DAYS.toMillis(days); 
		long hours = TimeUnit.MILLISECONDS.toHours(millis);
		millis -= TimeUnit.HOURS.toMillis(hours);
		long minutes = TimeUnit.MILLISECONDS.toMinutes(millis);
		millis -= TimeUnit.MINUTES.toMillis(minutes); 
		long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);

		logger.info ("  - Elapsed time: " + (histDataLoadStopTime - histDataLoadStartTime) + " ms - (" + hours + " hrs " + minutes + " min " + seconds + " secs)"); 
		logger.info ("**************************************************"); 
		logger.info ("");
		logger.info ("Calculations:");
		logger.info ("**************************************************"); 
		logger.info ("  - Start time  : " + new Timestamp(calculationStartTime)); 
		logger.info ("  - Stop time   : " + new Timestamp(calculationStopTime)); 

		millis = calculationStopTime - calculationStartTime;
		days = TimeUnit.MILLISECONDS.toDays(millis);
		millis -= TimeUnit.DAYS.toMillis(days); 
		hours = TimeUnit.MILLISECONDS.toHours(millis);
		millis -= TimeUnit.HOURS.toMillis(hours);
		minutes = TimeUnit.MILLISECONDS.toMinutes(millis);
		millis -= TimeUnit.MINUTES.toMillis(minutes); 
		seconds = TimeUnit.MILLISECONDS.toSeconds(millis);

		logger.info ("  - Elapsed time: " + (calculationStopTime - calculationStartTime) + " ms - (" + hours + " hrs " + minutes + " min " + seconds + " secs)"); 
		logger.info ("**************************************************");
		
		long totalHistoricalData = 0;

		if (historicalDataMap != null && historicalDataMap.size() > 0) {
			Iterator<Entry<String, List<FxRate>>> iter = historicalDataMap.entrySet().iterator();
			
			while (iter.hasNext()) {
	            Entry<String, List<FxRate>> entry = iter.next();
	            totalHistoricalData += ((List<FxRate>)entry.getValue()).size();
	        }
		}

		long totalCalculations = 0;
		long totalResults = 0;

		if (calcResultsMap != null && calcResultsMap.size() > 0) {
			Iterator<Entry<String, CalcResult>> iter = calcResultsMap.entrySet().iterator();
			
			while (iter.hasNext()) {
	            Entry<String, CalcResult> entry = iter.next();
	            totalCalculations += ((CalcResult)entry.getValue()).getTotalCalculations();
	            totalResults += ((CalcResult)entry.getValue()).getLevelResults().size();
	        }
		}
		logger.info ("");
		logger.info ("Total figures:");
		logger.info ("**************************************************");
		logger.info ("  - Total historical data : " + totalHistoricalData); 
		logger.info ("  - Total calculations    : " + totalCalculations); 
		logger.info ("  - Total results         : " + totalResults);
		logger.info ("**************************************************"); 
		logger.info ("");
		logger.info ("Results:");
		logger.info ("**************************************************");

		if (calcResultsMap != null && calcResultsMap.size() > 0) {

			for (String currency : currencyPairs) {
				
				if (calcResultsMap.containsKey(currency)) {
					printCurrencyLevels (currency, ((CalcResult)calcResultsMap.get(currency)).getLevelResults(), maxLevels);
				} else {
					printCurrencyLevels (currency, null, maxLevels);
				}
			}
		}
		logger.info ("**************************************************");
		logger.info ("");
	}
	
	// Print currency result levels
	private static void printCurrencyLevels (final String currency, final Map<String,Integer> levelsMap, final int maxLevels) {
		
		StringBuilder stringBuilder = new StringBuilder();
		
		for (int i=1; i <= maxLevels; i++) {
			stringBuilder.append(i + "-[");
			if (levelsMap != null && levelsMap.containsKey("UP-"+i)) {
				stringBuilder.append(levelsMap.get("UP-"+i));
			} else {
				stringBuilder.append("0");
			}
			stringBuilder.append("|");
			if (levelsMap != null && levelsMap.containsKey("DOWN-"+i)) {
				stringBuilder.append(levelsMap.get("DOWN-"+i));
			} else {
				stringBuilder.append("0");
			}
			stringBuilder.append("] ");
		}
		
		logger.info (currency + "-> " + stringBuilder.toString());
	}

}
