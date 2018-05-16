package executionservices;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datamodel.CalcResult;
import datamodel.FxRate;
import utils.ApplicationProperties;
import utils.DatabaseUtils;

public class RunnableWorkerThread implements Runnable {

	// Logger
	private static Logger logger = LoggerFactory.getLogger(RunnableWorkerThread.class);

	private String datasource;
	private String currentCurrency;
	private CountDownLatch latch;

	private Map<String, List<FxRate>> historicalDataMap;
	private Map<String, Integer> resultsMap = new HashMap<String, Integer>();
	private Map<String, CalcResult> calcResultsMap;
	
	private long elapsedTimeMillis;
	
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
		long totalHistDataLoaded;
		long calculationStartTime;	
		long calculationStopTime;
		long totalCalculations;
		
		long startTime = System.currentTimeMillis();
		
		try {
			
			// Load required properties
			float increase = (1+(ApplicationProperties.getFloatProperty("execution.increasePercentage"))/100);
			float decrease = (1-(ApplicationProperties.getFloatProperty("execution.decreasePercentage"))/100);
			int maxLevels = ApplicationProperties.getIntProperty("execution.maxLevels");
			String startDate = ApplicationProperties.getStringProperty("execution.startDate");
			String endDate = ApplicationProperties.getStringProperty("execution.endDate");
			
			logger.info ("Populating historical data for " + currentCurrency);
			histDataStartTime = System.currentTimeMillis();
			totalHistDataLoaded = populateHistoricalFxData(currentCurrency,startDate,endDate);
			histDataStopTime = System.currentTimeMillis();
			logger.info ("Historical data populated for " + currentCurrency);
			
			logger.info ("Starting calculations for " + currentCurrency);
			calculationStartTime = System.currentTimeMillis();
			totalCalculations = executeCalculations (currentCurrency, increase, decrease, maxLevels);
			calculationStopTime = System.currentTimeMillis();
			logger.info ("Calculations completed for " + currentCurrency);

			// Populates the Calculation Result Map
			calcResultsMap.put(currentCurrency, new CalcResult(currentCurrency, increase, decrease, maxLevels, histDataStartTime, histDataStopTime, totalHistDataLoaded, calculationStartTime, calculationStopTime, totalCalculations, resultsMap));
			
			logger.info ("Finished calculations [" + totalCalculations + "] in " + (calculationStopTime - calculationStartTime) + " ms");
			latch.countDown();
			
			long stopTime = System.currentTimeMillis(); 
			elapsedTimeMillis = stopTime - startTime;
			
		} catch (Exception e) { 
			e.printStackTrace(); 
		}
	}
	
	// Executes calculations
    public long executeCalculations (final String currentCurrency, float increase, float decrease, int maxLevels) {
    	
    	long totalCalculations = 0;
    	
		if (historicalDataMap.containsKey(currentCurrency)) {

			for (FxRate originalFxRate : historicalDataMap.get(currentCurrency)) {
				
				int positionId = originalFxRate.getPositionId();
				String currencyPair = originalFxRate.getCurrencyPair();
				float opening = originalFxRate.getOpen();
				
				logger.debug ("Processing " + currencyPair + "-" + positionId);
				
				FxRate targetFxRate = null;
				String previousFound = "";
				
				int indexUp = 1;
				int indexDown = 1;

				for (int i=positionId+1; i<historicalDataMap.get(currentCurrency).size(); i++) {
					targetFxRate = historicalDataMap.get(currentCurrency).get(i);
					
					if (originalFxRate.getCurrencyPair().equals(targetFxRate.getCurrencyPair())) {
					
						logger.debug ("Comparing against " + targetFxRate.getCurrencyPair() + "-" + targetFxRate.getPositionId());
						
						if ((targetFxRate.getHigh() > opening * increase) && (indexUp <= maxLevels)) {
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
						} else if ((targetFxRate.getLow() < opening * decrease) && (indexDown <= maxLevels)) {
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
		}
		return totalCalculations;
    }
	
	// Populates historical data and puts the objects into historical data list)
    // Depending on the datasource parameter, data could be retrieved from database (mysql) or files
    // FX Historical Data format: conversionDate,conversionTime,open,high,low,close
    public long populateHistoricalFxData (final String currentCurrency, final String startDate, final String endDate) {
    	
    	long result = 0;
    	
    	logger.info("Data source set to: " + datasource);

    	if ("database".equals(datasource)) {
    		// Populate historical data from mysql database
    		
    		historicalDataMap = DatabaseUtils.getHistoricalRates(currentCurrency, startDate, endDate);
    		
    		if (historicalDataMap != null && historicalDataMap.size() > 0) {
    			// There should be only 1 record in the map corresponding to the currentCurrency
   	            logger.info ("   " + currentCurrency + " -> total records loaded " + historicalDataMap.get(currentCurrency).size());
   	            result = historicalDataMap.get(currentCurrency).size();
    		}
    	} else {
    		logger.info("ONLY database datasource allowed");
    	}
    	
    	/*
    	else {
   	    	int totalCounter = 0;

    		// Populate historical data from files
    		historicalDataMap = new HashMap<String,List<FxRate>>();
    		
    		String fileName = null;
    		
        	logger.info("Populating historical data from file (ext. " + historicalDataFileExtension + ") from "+ historicalDataPath);
        	logger.info("Looking for " + currentCurrency + " file");
        	
        	List<String> dataFiles = GeneralUtils.getFilesFromPath(historicalDataPath,historicalDataFileExtension);
        	
        	for(String dataFile : dataFiles){
        		
        		fileName = dataFile.substring(0,dataFile.indexOf("."));
        		
        		if (currentCurrency.equals(fileName)) {
        		
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
    	*/

    	logger.info ("Populating historical data finished");
    	return result;
    }
    
	public long getElapsedTimeMillis () { 
		return this.elapsedTimeMillis; 
	}
}
