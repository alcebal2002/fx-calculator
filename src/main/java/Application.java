// /Library/Java/JavaVirtualMachines/jdk1.8.0_102.jdk/Contents/Home/jre/lib/rt.jar to be added to the classpath

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datamodel.CalcResult;
import executionservices.RejectedExecutionHandlerImpl;
import executionservices.RunnableWorkerThread;
import executionservices.SystemLinkedBlockingQueue;
import executionservices.SystemMonitorThread;
import executionservices.SystemThreadPoolExecutor;
import utils.ApplicationProperties;
import utils.DatabaseConnection;
import utils.DatabaseUtils;
import utils.GeneralUtils;

public class Application {

	// Logger
	private static Logger logger = LoggerFactory.getLogger(Application.class);

	// WorkerPool parameters 
	private static int poolCoreSize;	
	private static int poolMaxSize; 
	private static int queueCapacity; 
	private static int timeoutSecs; 
	private static int retrySleepTime; 
	private static int retryMaxAttempts; 
	private static int initialSleep; 
	private static int monitorSleep;

	// Execution time
	private static long applicationStartTime;	
	private static long applicationStopTime;	
	
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
	
	private static final String applicationId = (""+System.currentTimeMillis());

	
	// Lists and Maps
	private static Map<String,CalcResult> calcResultsMap = new HashMap<String,CalcResult>();
	
    public static void main (String args[]) {

    	applicationStartTime = System.currentTimeMillis();

		logger.info("Application started");
		logger.info("Loading application properties from " + ApplicationProperties.getPropertiesFile());

		// Load properties from file
		loadProperties ();
    	
		// Print parameters used
		printParameters ("Start");
		
		// Execute workers
		executeWorkers ();
        
		applicationStopTime = System.currentTimeMillis();

		// Print results
        printResults ();
		logger.info("Application finished");
		// Exit application
		System.exit(0);
    }

    
    private static void executeWorkers () {

		// RejectedExecutionHandler implementation 
		RejectedExecutionHandlerImpl rejectionHandler = new RejectedExecutionHandlerImpl(); 
		
		// Get the ThreadFactory implementation to use 
		ThreadFactory threadFactory = Executors.defaultThreadFactory();
		
		/* Define the BlockingQueue. 
		 * ArrayBlockingQueue to set a fixed capacity queue
		 * LinkedBlockingQueue to set an unbound capacity queue
		*/
		SystemLinkedBlockingQueue<Runnable> blockingQueue = new SystemLinkedBlockingQueue<Runnable>();		
		
		// Create the ThreadPoolExecutor
		SystemThreadPoolExecutor executorPool = new SystemThreadPoolExecutor(poolCoreSize, poolMaxSize, timeoutSecs, TimeUnit.SECONDS, blockingQueue, threadFactory, rejectionHandler); 

    	try { 

			logger.info ("Starting workers");
	
			CountDownLatch latch = new CountDownLatch(currencyPairs.size());
			
			List<String> existingCurrencies = DatabaseUtils.getExistingCurrencies();
					
			for (String currentCurrency : currencyPairs) {
				
			/*		
				if ((executorPool.getActiveCount() < executorPool.getMaximumPoolSize()) ||
						(blockingQueue.size() < queueCapacity)) { // For LinkedBlockingQueue 
			 */			

				if (existingCurrencies.indexOf(currentCurrency.toUpperCase()) != -1) {				
					executorPool.execute(new RunnableWorkerThread(datasource, currentCurrency, calcResultsMap, latch));
				} else {
					latch.countDown();
					logger.info(currentCurrency + " -> Table not present in database, ignoring...");
				}
			}

			// Start the monitoring thread 
			SystemMonitorThread monitor = new SystemMonitorThread(executorPool, monitorSleep, applicationId); 
			Thread monitorThread = new Thread(monitor); 
			monitorThread.start(); 

			logger.info("Waiting for all the Workers to finish");
			latch.await();
			logger.info("All workers finished");
			
			logger.info ("Shutting down monitor thread..."); 
			monitor.shutdown(); 
			
		} catch (Exception e) { 
			e.printStackTrace(); 
		} finally {
			DatabaseConnection.closeConnection();
		}
	} 
    
    private static void loadProperties () {

		poolCoreSize = ApplicationProperties.getIntProperty("workerpool.coreSize");
		poolMaxSize = ApplicationProperties.getIntProperty("workerpool.maxSize");
		queueCapacity = ApplicationProperties.getIntProperty("workerpool.queueCapacity");
		timeoutSecs = ApplicationProperties.getIntProperty("workerpool.timeoutSecs");
		retrySleepTime = ApplicationProperties.getIntProperty("workerpool.retrySleepTime");
		retryMaxAttempts = ApplicationProperties.getIntProperty("workerpool.retryMaxAttempts");
		monitorSleep = ApplicationProperties.getIntProperty("workerpool.monitorSleep");

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
		logger.info ("**************************************************"); 
		logger.info (title + " WorkerPool with the following parameters:"); 
		logger.info ("**************************************************"); 
		logger.info ("  - pool core size           : " + poolCoreSize); 
		logger.info ("  - pool max size            : " + poolMaxSize); 
		logger.info ("  - queue capacity           : " + queueCapacity); 
		logger.info ("  - timeout (secs)           : " + timeoutSecs); 
		logger.info ("  - retry sleep (ms)         : " + retrySleepTime); 
		logger.info ("  - retry max attempts       : " + retryMaxAttempts);
		logger.info ("  - initial sleep (secs)     : " + initialSleep); 
		logger.info ("  - monitor sleep (secs)     : " + monitorSleep); 
		logger.info ("**************************************************");

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

		long totalHistoricalData = 0;
		long totalCalculations = 0;
		long totalResults = 0;

		if (calcResultsMap != null && calcResultsMap.size() > 0) {
			Iterator<Entry<String, CalcResult>> iter = calcResultsMap.entrySet().iterator();
			
			while (iter.hasNext()) {
	            Entry<String, CalcResult> entry = iter.next();
	            totalCalculations += ((CalcResult)entry.getValue()).getTotalCalculations();
	            totalResults += ((CalcResult)entry.getValue()).getLevelResults().size();
	            totalHistoricalData += ((CalcResult)entry.getValue()).getTotalHistDataLoaded();
	        }
		}
		logger.info ("");
		logger.info ("Total figures:");
		logger.info ("**************************************************");
		logger.info ("  - Total historical data    : " + String.format("%,d", totalHistoricalData));
		logger.info ("  - Total calculations       : " + String.format("%,d", totalCalculations)); 
		logger.info ("  - Total results            : " + String.format("%,d", totalResults));
		logger.info ("  - Elapsed time             : " + GeneralUtils.printElapsedTime (applicationStartTime,applicationStopTime)); 
		logger.info ("**************************************************"); 
		logger.info ("");
		logger.info ("Results:");
		logger.info ("**************************************************");
		logger.info ("Currency -> Level-[UP|DOWN|TOTAL|%]");
		logger.info ("");
		
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
		
		double referenceLevel = 0;

		for (int i=1; i <= maxLevels; i++) {
			long total=0;
			stringBuilder.append(i + "-[");
			if (levelsMap != null && levelsMap.containsKey("UP-"+i)) {
				stringBuilder.append(levelsMap.get("UP-"+i));
				total += levelsMap.get("UP-"+i);
			} else {
				stringBuilder.append("0");
			}
			stringBuilder.append("|");
			if (levelsMap != null && levelsMap.containsKey("DOWN-"+i)) {
				stringBuilder.append(levelsMap.get("DOWN-"+i));
				total += levelsMap.get("DOWN-"+i);
			} else {
				stringBuilder.append("0");
			}
			stringBuilder.append("|");
			stringBuilder.append(total);
			stringBuilder.append("|");
			if (i==1) referenceLevel = total;
			stringBuilder.append(new DecimalFormat("#.##").format(total*100/referenceLevel));
			stringBuilder.append("] ");
		}
		
		logger.info (currency + "-> " + stringBuilder.toString());
	}

}
