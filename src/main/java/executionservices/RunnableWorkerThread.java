package executionservices;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVReader;

import datamodel.CalcResult;
import datamodel.FxRate;
import utils.ApplicationProperties;
import utils.DatabaseUtils;
import utils.GeneralUtils;

public class RunnableWorkerThread implements Runnable {

	// Logger
	private static Logger logger = LoggerFactory.getLogger(RunnableWorkerThread.class);

	private String datasource;
	private String currentCurrency;
	private CountDownLatch latch;

	private Map<String, List<FxRate>> historicalDataMap = new HashMap<String, List<FxRate>>();
	private Map<String, Integer> resultsMap = new HashMap<String, Integer>();
	private Map<String, CalcResult> calcResultsMap;
	
	private long elapsedTimeMillis;
	private long totalHistDataLoaded;
	private long totalCalculations;
	private long totalResults;


	public RunnableWorkerThread ( final String datasource, final String currentCurrency, Map<String, CalcResult> calcResultsMap, CountDownLatch latch){
		this.datasource = datasource;
		this.currentCurrency = currentCurrency;
		this.calcResultsMap = calcResultsMap;
		this.latch = latch;
	}
	
	@Override
	public void run() {
		
		long histDataStartTime;	
		long histDataStopTime;
		long calculationStartTime;
		long calculationStopTime;

		long startTime = System.currentTimeMillis();
		
		try {
			
			// Load required properties
			float increase = (1+(ApplicationProperties.getFloatProperty("execution.increasePercentage"))/100);
			float decrease = (1-(ApplicationProperties.getFloatProperty("execution.decreasePercentage"))/100);
			int maxLevels = ApplicationProperties.getIntProperty("execution.maxLevels");
			String startDate = ApplicationProperties.getStringProperty("execution.startDate");
			String endDate = ApplicationProperties.getStringProperty("execution.endDate");

			if (checkIfCurrencyExists (currentCurrency)) {

				logger.info ("Populating historical data for " + currentCurrency);
				histDataStartTime = System.currentTimeMillis();
				totalHistDataLoaded = populateHistoricalFxData(currentCurrency,startDate,endDate);
				histDataStopTime = System.currentTimeMillis();
				logger.info ("Historical data populated for " + currentCurrency);

				logger.info ("Starting calculations for " + currentCurrency);
				calculationStartTime = System.currentTimeMillis();
				totalCalculations = executeCalculations (currentCurrency, increase, decrease, maxLevels);
				calculationStopTime = System.currentTimeMillis();

				totalResults = resultsMap.size();

				logger.debug ("Populating Calculation Result Map for " + currentCurrency);
				// Populates the Calculation Result Map
				calcResultsMap.put(currentCurrency, new CalcResult(currentCurrency, increase, decrease, maxLevels, histDataStartTime, histDataStopTime, totalHistDataLoaded, calculationStartTime, calculationStopTime, totalCalculations, resultsMap));

				logger.info ("Finished calculations for " + currentCurrency + "[" + totalCalculations + "] in " + (calculationStopTime - calculationStartTime) + " ms");
			} else {
				logger.error("No available data for " + currentCurrency);
			}

			latch.countDown();
			
			long stopTime = System.currentTimeMillis(); 
			elapsedTimeMillis = stopTime - startTime;
			
		} catch (Exception e) { 
			e.printStackTrace(); 
		}
	}

	public boolean checkIfCurrencyExists (final String currentCurrency) {

		boolean exists = false;

		if ("database".equals(datasource)) {
			exists = DatabaseUtils.checkCurrencyTableExists(currentCurrency);
		} else {
			exists = GeneralUtils.checkIfFileExists(currentCurrency);
		}
		return exists;
	}

	// Executes calculations
    public long executeCalculations (final String currentCurrency, float increase, float decrease, int maxLevels) {
    	
    	long totalCalculations = 0;
    	
		if (historicalDataMap.containsKey(currentCurrency)) {

			for (FxRate originalFxRate : historicalDataMap.get(currentCurrency)) {
				
				int positionId = originalFxRate.getPositionId();
				float opening = originalFxRate.getOpen();
				
				logger.debug ("Processing " + currentCurrency + "-" + positionId);
				
				FxRate targetFxRate = null;
				String previousFound = "";
				
				int indexUp = 1;
				int indexDown = 1;

				for (int i=positionId+1; i<historicalDataMap.get(currentCurrency).size(); i++) {
					targetFxRate = historicalDataMap.get(currentCurrency).get(i);
					
					logger.debug ("Comparing against " + targetFxRate.getCurrencyPair() + "-" + targetFxRate.getPositionId());

					if ((targetFxRate.getHigh() > (opening * increase)) && (indexUp <= maxLevels)) {
						if (("DOWN").equals(previousFound)) {
							break;
						}

						if (resultsMap.containsKey("UP-"+indexUp)) {
							resultsMap.put("UP-"+indexUp,resultsMap.get("UP-"+indexUp)+1);
						} else {
							resultsMap.put("UP-"+indexUp,1);
						}

						previousFound = "UP";
						opening = opening * increase;
						indexUp++;
					} else if ((targetFxRate.getLow() < (opening * decrease)) && (indexDown <= maxLevels)) {
						if (("UP").equals(previousFound)) {
							break;
						}

						if (resultsMap.containsKey("DOWN-"+indexDown)) {
							resultsMap.put("DOWN-"+indexDown,resultsMap.get("DOWN-"+indexDown)+1);
						} else {
							resultsMap.put("DOWN-"+indexDown,1);
						}

						previousFound = "DOWN";
						opening = opening * decrease;
						indexDown++;
					}
					totalCalculations++;
				}
			}
		}
		return totalCalculations;
    }
	
	// Populates historical data and puts the objects into historical data list)
    // Depending on the datasource parameter, data could be retrieved from database (mysql) or files
    // FX Historical Data format: conversionDate,conversionTime,open,high,low,close
    public long populateHistoricalFxData (final String currentCurrency, final String startDate, final String endDate) {
    	
    	long result = 0;
    	
    	logger.debug("Data source set to: " + datasource);

    	if ("database".equals(datasource)) {
    		// Populate historical data from mysql database
    		
    		historicalDataMap = DatabaseUtils.getHistoricalRates(currentCurrency, startDate, endDate);
    		
    		if (historicalDataMap != null && historicalDataMap.size() > 0) {
    			// There should be only 1 record in the map corresponding to the currentCurrency
   	            logger.info (currentCurrency + " -> total records loaded " + historicalDataMap.get(currentCurrency).size());
    		}
    	} else {

   	    	int totalCounter = 0;
   	    	int lineNumber = 0;

			String historicalDataPath = ApplicationProperties.getStringProperty("main.historicalDataPath");
			String historicalDataFileExtension = ApplicationProperties.getStringProperty("main.historicalDataFileExtension");
			String historicalDataSeparator = ApplicationProperties.getStringProperty("main.historicalDataSeparator");
			int printAfter = ApplicationProperties.getIntProperty("test.printAfter");

			String fileName = historicalDataPath + currentCurrency + historicalDataFileExtension;
    		
        	logger.info("Populating historical data from file (" + fileName + "). Fields separated by " + historicalDataSeparator.charAt(0));
        	
        	try {
        		CSVReader reader = new CSVReader(new FileReader(fileName), historicalDataSeparator.charAt(0));
    	        String [] nextLine;
    	        while ((nextLine = reader.readNext()) != null) {
    	        	
    	        	FxRate fxRate = new FxRate (currentCurrency,nextLine,totalCounter,startDate,endDate);
    	        	
    	        	// Check if the fxRate has been created or excluded due to the date filtering
    	        	if (currentCurrency.equals(fxRate.getCurrencyPair())) {
    					if (!historicalDataMap.containsKey(currentCurrency)) {
    						historicalDataMap.put(currentCurrency, new ArrayList<FxRate>());							
    					}
    					(historicalDataMap.get(currentCurrency)).add(fxRate);

    					if (totalCounter%printAfter == 0) {
        		        	logger.debug ("  " + currentCurrency + " -> loaded " + totalCounter + " records so far");
        				}
    					totalCounter++;
    	        	}
    	        	lineNumber++;
    	        }
    	        logger.info (currentCurrency + " -> total records loaded " + totalCounter);
    	        reader.close();
    	    	
        	} catch (Exception ex) {
        		logger.error ("Exception in file " + fileName + " - line " + lineNumber + " - " + ex.getClass() + " - " + ex.getMessage());
        	}
    	}
    	
    	if (historicalDataMap.containsKey(currentCurrency)) {
    		result = historicalDataMap.get(currentCurrency).size();
    	}
    	
    	return result;
    }

	public long getTotalResutls () {
		return this.totalResults;
	}
	public long getTotalCalculations () {
		return this.totalCalculations;
	}
	public long getTotalHistDataLoaded () {
		return this.totalHistDataLoaded;
	}
	public long getElapsedTimeMillis () {
		return this.elapsedTimeMillis;
	}
}
